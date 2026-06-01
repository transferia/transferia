package reader

import (
	"context"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
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

const timeLocalLayout = "02/Jan/2006:15:04:05 -0700"

var _ s3_reader.S3Reader = (*NginxReader)(nil)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatNginx, NewNginxReader, NewNginxSchemaResolver)
}

type NginxReader struct {
	s3RawReaderBuilder      s3raw.S3RawReaderBuilder
	table                   abstract.TableID
	bucket                  string
	client                  s3iface.S3API
	logger                  log.Logger
	metrics                 *stats.SourceStats
	hideSystemCols          bool
	batchSize               int
	blockSize               int64
	pathPrefix              string
	pathPattern             string
	unparsedPolicy          s3_model.UnparsedPolicy
	unexpectedFieldBehavior s3_model.NginxUnexpectedFieldBehavior

	compiledFormat *compiledNginxFormat
}

//nolint:descriptiveerrors
func (r *NginxReader) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(r.s3RawReaderBuilder).BuildReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("could not build s3 raw reader, err: %w", err)
	}
	return s3RawReader, nil
}

//nolint:descriptiveerrors
func (r *NginxReader) Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	if schema == nil || len(schema.Columns()) == 0 {
		return nil
	}
	fastCols := schema.FastColumns()
	colNames := yslices.Map(schema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })

	s3RawReader, err := r.newS3RawReader(ctx, filePath)
	if err != nil {
		return reader_error.NewReaderErrorFromObjectOpen("nginx.parser.Read.newS3RawReader", filePath, err)
	}

	lineCounter := uint64(1)
	chunkReader := s3_reader.NewChunkReader(s3RawReader, int(r.blockSize), r.logger)
	defer chunkReader.Close()

	for lastRound := false; !lastRound; {
		if ctx.Err() != nil {
			r.logger.Info("Read canceled")
			return nil
		}

		if err := chunkReader.ReadNextChunk(); err != nil {
			return reader_error.NewReaderErrorTransport("chunk.ReadNextChunk", filePath, err)
		}

		if chunkReader.IsEOF() {
			if len(chunkReader.Data()) == 0 {
				break
			}
			lastRound = true
		}

		data := string(chunkReader.Data())
		var buff []abstract.ChangeItem
		var currentSize int64

		// Find the last newline to determine the boundary of complete lines.
		lastNewlinePos := strings.LastIndexByte(data, '\n')
		var processable string
		if lastNewlinePos >= 0 {
			processable = data[:lastNewlinePos+1]
		} else if lastRound {
			processable = data
		} else {
			// No complete line in this chunk, save everything for the next chunk.
			chunkReader.FillBuffer([]byte(data))
			if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
				return wrapNginxReadFlushChunk(err)
			}
			continue
		}

		for line := range strings.SplitSeq(processable, "\n") {
			line = strings.TrimRight(line, "\r")
			if strings.TrimSpace(line) == "" {
				continue
			}

			fieldValues, consumed, err := r.compiledFormat.parseEntry(line)
			if err != nil {
				errorData := reader_error.NewReaderErrorDataRecord("nginx.parseEntry", filePath, lineCounter, err)
				items, handleErr := reader_error.HandleDataError(r.table, r.unparsedPolicy, errorData)
				if handleErr != nil {
					return handleErr
				}
				buff = append(buff, items...)
				lineCounter++
				continue
			}

			if err := checkUnexpectedFields(line, consumed, r.unexpectedFieldBehavior); err != nil {
				errorData := reader_error.NewReaderErrorDataRecord("nginx.checkUnexpectedFields", filePath, lineCounter, err)
				items, handleErr := reader_error.HandleDataError(r.table, r.unparsedPolicy, errorData)
				if handleErr != nil {
					return handleErr
				}
				buff = append(buff, items...)
				lineCounter++
				continue
			}

			currentSize += int64(len(line))

			changeItem, err := r.buildCI(fieldValues, filePath, s3RawReader.LastModified(), lineCounter, schema, fastCols, colNames)
			if err != nil {
				errorData, ok := reader_error.AsReaderErrorData(err)
				if !ok {
					errorData = reader_error.NewReaderErrorDataRecord("nginx.buildCI", filePath, lineCounter, err)
				}
				items, handleErr := reader_error.HandleDataError(r.table, r.unparsedPolicy, errorData)
				if handleErr != nil {
					return handleErr
				}
				buff = append(buff, items...)
				lineCounter++
				continue
			}

			lineCounter++
			buff = append(buff, *changeItem)

			if len(buff) > r.batchSize {
				if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
					return wrapNginxReadFlushChunk(err)
				}
				currentSize = 0
				buff = nil
			}
		}

		// Save incomplete trailing line (after last \n) for the next chunk.
		if lastNewlinePos >= 0 && lastNewlinePos+1 < len(data) {
			chunkReader.FillBuffer([]byte(data[lastNewlinePos+1:]))
		} else {
			chunkReader.FillBuffer(nil)
		}

		if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
			return wrapNginxReadFlushChunk(err)
		}
	}

	return nil
}

//nolint:descriptiveerrors
func (r *NginxReader) buildCI(
	fieldValues []string,
	filePath string,
	lastModified time.Time,
	lineCounter uint64,
	schema *abstract.TableSchema,
	fastCols abstract.FastTableSchema,
	colNames []string,
) (*abstract.ChangeItem, reader_error.ReaderError) {
	changeItem, err := r.constructCI(fieldValues, filePath, lastModified, lineCounter, schema, colNames)
	if err != nil {
		return nil, reader_error.WrapReaderError("nginx.buildCI.constructCI", err)
	}

	if err := strictify.Strictify(changeItem, fastCols); err != nil {
		return nil, reader_error.NewReaderErrorDataRecord("nginx.buildCI.strictify", filePath, lineCounter, err)
	}

	return changeItem, nil
}

//nolint:descriptiveerrors
func (r *NginxReader) constructCI(
	fieldValues []string,
	fileName string,
	lModified time.Time,
	rowNumber uint64,
	schema *abstract.TableSchema,
	colNames []string,
) (*abstract.ChangeItem, reader_error.ReaderError) {
	vals := make([]any, len(schema.Columns()))
	for i, col := range schema.Columns() {
		if s3_reader.SystemColumnNames[col.ColumnName] {
			if r.hideSystemCols {
				continue
			}
			switch col.ColumnName {
			case s3_reader.FileNameSystemCol:
				vals[i] = fileName
			case s3_reader.RowIndexSystemCol:
				vals[i] = rowNumber
			}
			continue
		}

		index, err := strconv.Atoi(col.Path)
		if err != nil {
			return nil, reader_error.NewReaderErrorDataRecord("nginx.constructCI.colPath", fileName, rowNumber, err)
		}
		if index < 0 || index >= len(fieldValues) {
			vals[i] = abstract.DefaultValue(&col)
			continue
		}

		converted, err := convertNginxValue(fieldValues[index], col)
		if err != nil {
			return nil, reader_error.NewReaderErrorDataRecord("nginx.constructCI.convert", fileName, rowNumber, err)
		}
		vals[i] = converted
	}

	return &abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   uint64(lModified.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       r.table.Namespace,
		Table:        r.table.Name,
		PartID:       fileName,
		ColumnNames:  colNames,
		ColumnValues: vals,
		TableSchema:  schema,
		OldKeys:      abstract.EmptyOldKeys(),
		Size:         abstract.RawEventSize(util.DeepSizeof(vals)),
		TxID:         "",
		Query:        "",
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    "",
			PartitionNum: 0,
			Offset:       0,
			Index:        0,
		},
	}, nil
}

// EstimateRowsCountAllObjects - Row count estimation
func (r *NginxReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, s3_reader.IsNotEmpty)
	if err != nil {
		return 0, xerrors.Errorf("ListFiles returned error, err: %w", err)
	}
	return r.estimateRows(ctx, files)
}

func (r *NginxReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	return r.estimateRows(ctx, []*aws_s3.Object{obj})
}

func (r *NginxReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	totalSize, sampleReader, err := estimators.EstimateTotalSizeInBytes(ctx, r.logger, files, r.newS3RawReader)
	if err != nil {
		return 0, xerrors.Errorf("unable to EstimateTotalSizeInBytes, err: %w", err)
	}
	if totalSize != 0 && sampleReader != nil {
		chunkReader := s3_reader.NewChunkReader(sampleReader, int(r.blockSize), r.logger)
		defer chunkReader.Close()

		if err := chunkReader.ReadNextChunk(); err != nil && !xerrors.Is(err, io.EOF) {
			return 0, xerrors.Errorf("unable to ReadNextChunk, err: %w", err)
		}
		if len(chunkReader.Data()) != 0 {
			data := string(chunkReader.Data())
			entries := 0
			for line := range strings.SplitSeq(data, "\n") {
				line = strings.TrimRight(line, "\r")
				if strings.TrimSpace(line) == "" {
					continue
				}
				if _, _, err := r.compiledFormat.parseEntry(line); err != nil {
					break
				}
				entries++
			}
			if entries > 0 {
				bytesPerEntry := float64(len(data)) / float64(entries)
				return uint64(math.Ceil(float64(totalSize) / bytesPerEntry)), nil
			}
		}
	}
	return 0, nil
}

func NewNginxReader(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3Reader, error) {
	if src == nil || src.Format.NginxSetting == nil {
		return nil, xerrors.New("nginx.reader is nil")
	}

	compiled, err := compileFormat(src.Format.NginxSetting.Format)
	if err != nil {
		return nil, xerrors.Errorf("unable to compileFormat, err: %w", err)
	}

	return &NginxReader{
		s3RawReaderBuilder: s3RawReaderBuilder,
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:                  src.Bucket,
		client:                  s3RawReaderBuilder.BuildClient(sess),
		logger:                  lgr,
		metrics:                 metrics,
		hideSystemCols:          src.HideSystemCols,
		batchSize:               src.ReadBatchSize,
		blockSize:               src.Format.NginxSetting.BlockSize,
		pathPrefix:              src.PathPrefix,
		pathPattern:             src.PathPattern,
		unparsedPolicy:          src.UnparsedPolicy,
		unexpectedFieldBehavior: src.Format.NginxSetting.UnexpectedFieldBehavior,
		compiledFormat:          compiled,
	}, nil
}
