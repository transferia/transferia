package json

import (
	"context"
	"time"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/goccy/go-json"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/estimators"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	_ s3_reader.S3Reader = (*JSONLineReader)(nil)

	RestColumnName = "rest"
)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatJSONLine, NewJSONLineReader, NewJSONLineSchemaResolver)
}

type JSONLineReader struct {
	s3RawReaderBuilder      s3raw.S3RawReaderBuilder
	table                   abstract.TableID
	bucket                  string
	client                  s3iface.S3API
	logger                  log.Logger
	hideSystemCols          bool
	batchSize               int
	pathPrefix              string
	newlinesInValue         bool
	unexpectedFieldBehavior s3_model.UnexpectedFieldBehavior
	blockSize               int64
	pathPattern             string
	metrics                 *stats.SourceStats
	unparsedPolicy          s3_model.UnparsedPolicy
}

func (r *JSONLineReader) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(r.s3RawReaderBuilder).BuildReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("failed to new S3RawReader for key: %s, err: %w", filePath, err)
	}
	return s3RawReader, nil
}

func (r *JSONLineReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	readAllImpl := func(in []byte) ([]string, uint64, error) {
		lines, bytesRead := readAllMultilineLines(in)
		return lines, bytesRead, nil
	}
	return estimators.EstimateRows(ctx, r.logger, files, int(r.blockSize), r.newS3RawReader, readAllImpl)
}

func (r *JSONLineReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to EstimateRowsCountOneObject for key: %s, err: %w", *obj.Key, err)
	}
	return res, nil
}

func (r *JSONLineReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, s3_reader.IsNotEmpty)
	if err != nil {
		return 0, xerrors.Errorf("failed to ListFiles, err: %w", err)
	}

	res, err := r.estimateRows(ctx, files)
	if err != nil {
		return 0, xerrors.Errorf("failed to estimateRows files, err: %w", err)
	}
	return res, nil
}

//nolint:descriptiveerrors
func (r *JSONLineReader) Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	if schema == nil || len(schema.Columns()) == 0 {
		return reader_error.NewReaderErrorConfig("jsonl.Read.schema", xerrors.New("schema is nil or has no columns"))
	}
	fastCols := schema.FastColumns()

	s3RawReader, err2 := r.newS3RawReader(ctx, filePath)
	if err2 != nil {
		return reader_error.NewReaderErrorTransport("jsonl.Read.newS3RawReader", filePath, err2)
	}

	offset := uint64(0)
	lineCounter := uint64(1)
	var readBytes uint64
	var lines []string
	chunkReader := s3_reader.NewChunkReader(s3RawReader, int(r.blockSize), r.logger)
	defer chunkReader.Close()

	skipReadBytes := uint64(0)
	for lastRound := false; !lastRound; {
		if ctx.Err() != nil {
			r.logger.Info("Read canceled")
			return nil
		}
		if err := chunkReader.ReadNextChunk(); err != nil {
			return reader_error.NewReaderErrorTransport("chunk.ReadNextChunk", filePath, err)
		}
		data := chunkReader.Data()
		if len(data) == 0 && chunkReader.IsEOF() {
			if offset == 0 {
				errData := reader_error.NewReaderErrorDataSchema(
					"jsonl.Read.empty_object",
					filePath,
					xerrors.New("empty jsonl object"),
				)
				items, handleErr := reader_error.HandleDataError(r.table, r.unparsedPolicy, errData)
				if handleErr != nil {
					return handleErr
				}
				if len(items) != 0 {
					if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, 0, items, pusher); err != nil {
						return wrapJSONLReadFlushChunk("empty", err)
					}
				}
			}
			break
		}
		if chunkReader.IsEOF() && len(data) != 0 {
			lastRound = true
		}
		if uint64(len(data)) < skipReadBytes {
			skipReadBytes -= uint64(len(data))
			continue
		}
		data = data[skipReadBytes:]
		if r.newlinesInValue {
			lines, readBytes = readAllMultilineLines(data)
		} else {
			var err2 reader_error.ReaderError = nil
			lines, readBytes, err2 = readAllLines(data, filePath)
			if err2 != nil {
				if handled, res := s3_reader.TryHandleDataErrorWithFlush(
					ctx,
					r.table,
					r.unparsedPolicy,
					filePath,
					lineCounter,
					0,
					err2,
					pusher,
					func(e reader_error.ReaderError) reader_error.ReaderError {
						return wrapJSONLReadFlushChunk("1", e)
					},
				); handled {
					return res
				}
				return reader_error.WrapReaderError("jsonl.Read.readAllLines", err2)
			}
		}

		offset += readBytes
		if readBytes > uint64(len(data)) {
			skipReadBytes = readBytes - uint64(len(data))
			readBytes = uint64(len(data))
		} else {
			skipReadBytes = 0
		}
		chunkReader.FillBuffer(data[readBytes:])
		var buff []abstract.ChangeItem
		var currentSize int64
		for _, line := range lines {
			changeItem, err := r.doParse(line, filePath, s3RawReader.LastModified(), lineCounter, schema, fastCols)
			if err != nil {
				errorData, ok := reader_error.AsReaderErrorData(err)
				if !ok {
					errorData = reader_error.NewReaderErrorDataRecord("jsonl.doParse", filePath, lineCounter, err)
				}
				items, handleErr := reader_error.HandleDataError(r.table, r.unparsedPolicy, errorData)
				if handleErr != nil {
					return handleErr
				}
				for i := range items {
					it := items[i]
					currentSize += int64(it.Size.Values)
					buff = append(buff, it)
				}
				lineCounter++
				if len(buff) > r.batchSize {
					if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
						return wrapJSONLReadFlushChunk("2", err)
					}
					currentSize = 0
					buff = make([]abstract.ChangeItem, 0)
				}
				continue
			}
			currentSize += int64(changeItem.Size.Values)
			lineCounter++
			buff = append(buff, *changeItem)
			if len(buff) > r.batchSize {
				if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
					return wrapJSONLReadFlushChunk("3", err)
				}
				currentSize = 0
				buff = make([]abstract.ChangeItem, 0)
			}
		}
		if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
			return wrapJSONLReadFlushChunk("4", err)
		}
	}

	return nil
}

func (r *JSONLineReader) doParse(
	line string,
	filePath string,
	lastModified time.Time,
	lineCounter uint64,
	schema *abstract.TableSchema,
	fastCols abstract.FastTableSchema,
) (*abstract.ChangeItem, error) {
	row := make(map[string]any)
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return nil, xerrors.Errorf("json.Unmarshal returned error for filePath: %s, err: %w", filePath, err)
	}

	changeItem, err := r.constructCI(row, filePath, lastModified, lineCounter, schema)
	if err != nil {
		return nil, err
	}

	if err := strictify.Strictify(changeItem, fastCols); err != nil {
		return nil, xerrors.Errorf("strictify.Strictify returned error for filePath: %s, err: %w", filePath, err)
	}
	return changeItem, nil
}

func (r *JSONLineReader) constructCI(row map[string]any, fileName string, lastModified time.Time, idx uint64, schema *abstract.TableSchema) (*abstract.ChangeItem, error) {
	colNames := yslices.Map(schema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })
	vals := make([]any, len(schema.Columns()))
	rest := make(map[string]any)
	for key, val := range row {
		known := false
		for _, col := range schema.Columns() {
			if col.ColumnName == key {
				known = true
				break
			}
		}
		if !known {
			if r.unexpectedFieldBehavior == s3_model.Infer {
				rest[key] = val
			} else if r.unexpectedFieldBehavior == s3_model.Ignore {
				continue
			} else {
				return nil, xerrors.Errorf("unknown column type: %s", key)
			}
		}
	}
	// TODO: add support for col.Path

	isSystemCol := func(colName string) bool {
		switch colName {
		case s3_reader.FileNameSystemCol:
			return true
		case s3_reader.RowIndexSystemCol:
			return true
		default:
			return false
		}
	}

	for i, col := range schema.Columns() {
		if isSystemCol(col.ColumnName) {
			if r.hideSystemCols {
				continue
			}
			switch col.ColumnName {
			case s3_reader.FileNameSystemCol:
				vals[i] = fileName
				continue
			case s3_reader.RowIndexSystemCol:
				vals[i] = idx
				continue
			}
		}
		val, ok := row[col.ColumnName]
		if !ok {
			if col.ColumnName == RestColumnName && r.unexpectedFieldBehavior == s3_model.Infer {
				vals[i] = abstract.Restore(col, rest)
			} else {
				vals[i] = nil
			}
			continue
		}
		vals[i] = val
	}

	return &abstract.ChangeItem{
		ID:               0,
		LSN:              0,
		CommitTime:       uint64(lastModified.UnixNano()),
		Counter:          0,
		Kind:             abstract.InsertKind,
		Schema:           r.table.Namespace,
		Table:            r.table.Name,
		PartID:           fileName,
		ColumnNames:      colNames,
		ColumnValues:     vals,
		TableSchema:      schema,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.RawEventSize(util.DeepSizeof(vals)),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}, nil
}

func NewJSONLineReader(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3Reader, error) {
	if src == nil || src.Format.JSONLSetting == nil {
		return nil, xerrors.New("jsonl.reader must set format.JSONLSetting")
	}

	jsonlSettings := src.Format.JSONLSetting

	return &JSONLineReader{
		s3RawReaderBuilder: s3RawReaderBuilder,

		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		}, bucket: src.Bucket,

		client: s3RawReaderBuilder.BuildClient(sess),
		logger: lgr,

		hideSystemCols: src.HideSystemCols,
		batchSize:      src.ReadBatchSize,
		pathPrefix:     src.PathPrefix,

		newlinesInValue:         jsonlSettings.NewlinesInValue,
		unexpectedFieldBehavior: jsonlSettings.UnexpectedFieldBehavior,
		blockSize:               jsonlSettings.BlockSize, pathPattern: src.PathPattern,

		metrics:        metrics,
		unparsedPolicy: src.UnparsedPolicy,
	}, nil
}
