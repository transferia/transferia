package line

import (
	"context"
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

var _ s3_reader.S3Reader = (*LineReader)(nil)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatLine, NewLineReader, NewLineSchemaResolver)
}

// LineReader reads line-delimited blobs; schema is supplied per Read (not stored).
type LineReader struct {
	s3RawReaderBuilder s3raw.S3RawReaderBuilder
	table              abstract.TableID
	bucket             string
	client             s3iface.S3API
	logger             log.Logger
	metrics            *stats.SourceStats
	batchSize          int
	blockSize          int64
	pathPrefix         string
	pathPattern        string
	hideSystemCols     bool
	unparsedPolicy     s3_model.UnparsedPolicy
}

func (r *LineReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, s3_reader.IsNotEmpty)
	if err != nil {
		return 0, xerrors.Errorf("listing objects failed: %w", err)
	}

	res, err := r.estimateRows(ctx, files)
	if err != nil {
		return 0, xerrors.Errorf("estimating rows failed: %w", err)
	}

	return res, nil
}

func (r *LineReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s, err: %w", *obj.Key, err)
	}
	return res, nil
}

func (r *LineReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	return estimators.EstimateRows(ctx, r.logger, files, int(r.blockSize), r.newS3RawReader, readLines)
}

func (r *LineReader) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(r.s3RawReaderBuilder).BuildReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("coalesce failed, err: %w", err)
	}

	return s3RawReader, nil
}

//nolint:descriptiveerrors
func (r *LineReader) Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	if schema == nil || len(schema.Columns()) == 0 {
		return nil
	}
	fastCols := schema.FastColumns()
	columnNames := yslices.Map(schema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })

	s3RawReader, err := r.newS3RawReader(ctx, filePath)
	if err != nil {
		return reader_error.NewReaderErrorFromObjectOpen("line.Read.newS3RawReader", filePath, err)
	}

	lineCounter := uint64(1)
	var readBytes uint64
	var lines []string
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

		if chunkReader.IsEOF() && len(chunkReader.Data()) != 0 {
			lastRound = true
		}

		lines, readBytes, err = readLines(chunkReader.Data())
		if err != nil {
			dataError := reader_error.NewReaderErrorDataFile("line.Read.readLines", filePath, err)
			if handled, res := s3_reader.TryHandleDataErrorWithFlush(ctx, r.table, r.unparsedPolicy, filePath, lineCounter, 0, dataError, pusher, wrapLineReadFlushChunk); handled {
				return res
			}
		}

		chunkReader.FillBuffer(chunkReader.Data()[readBytes:])
		var buff []abstract.ChangeItem
		var currentSize int64

		for _, line := range lines {
			if len(strings.TrimSpace(line)) == 0 {
				continue
			}

			changeItems, err := r.doParse(line, filePath, s3RawReader.LastModified(), lineCounter, schema, fastCols, columnNames)
			if err != nil {
				errorData := reader_error.NewReaderErrorDataRecord("line.doParse", filePath, lineCounter, err)
				items, handleErr := reader_error.HandleDataError(r.table, r.unparsedPolicy, errorData)
				if handleErr != nil {
					return handleErr
				}

				for i := range items {
					currentChangeItem := items[i]
					currentSize += int64(currentChangeItem.Size.Values)
					buff = append(buff, currentChangeItem)
				}
				lineCounter++

				if len(buff) > r.batchSize {
					if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
						return wrapLineReadFlushChunk(err)
					}
					currentSize = 0
					buff = make([]abstract.ChangeItem, 0)
				}
				continue
			}

			lineCounter++
			buff = append(buff, *changeItems)
			currentSize += int64(changeItems.Size.Values)

			if len(buff) > r.batchSize {
				if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
					return wrapLineReadFlushChunk(err)
				}
				currentSize = 0
				buff = make([]abstract.ChangeItem, 0)
			}
		}

		if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
			return wrapLineReadFlushChunk(err)
		}
	}

	return nil
}

func (r *LineReader) doParse(
	line string,
	filePath string,
	lastModified time.Time,
	lineCounter uint64,
	schema *abstract.TableSchema,
	fastCols abstract.FastTableSchema,
	columnNames []string,
) (*abstract.ChangeItem, error) {
	changeItem, err := r.constructCI(line, filePath, lastModified, lineCounter, schema, columnNames)
	if err != nil {
		return nil, err
	}

	if err := strictify.Strictify(changeItem, fastCols); err != nil {
		return nil, xerrors.Errorf("strictify.Strictify returned error, err: %w", err)
	}

	return changeItem, nil
}

func (r *LineReader) constructCI(
	line string,
	fileName string,
	lastModified time.Time,
	idx uint64,
	schema *abstract.TableSchema,
	columnNames []string,
) (*abstract.ChangeItem, error) {
	colSchemas := schema.Columns()
	values := make([]any, len(colSchemas))
	for i, col := range colSchemas {
		switch col.ColumnName {
		case s3_reader.FileNameSystemCol:
			if r.hideSystemCols {
				values[i] = nil
			} else {
				values[i] = fileName
			}
		case s3_reader.RowIndexSystemCol:
			if r.hideSystemCols {
				values[i] = nil
			} else {
				values[i] = idx
			}
		default:
			values[i] = line
		}
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
		ColumnNames:      columnNames,
		ColumnValues:     values,
		TableSchema:      schema,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.RawEventSize(util.DeepSizeof(values)),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}, nil
}

func NewLineReader(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3Reader, error) {
	return &LineReader{
		s3RawReaderBuilder: s3RawReaderBuilder,
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:         src.Bucket,
		client:         s3RawReaderBuilder.BuildClient(sess),
		logger:         lgr,
		metrics:        metrics,
		batchSize:      0,
		blockSize:      1 * 1024 * 1024, // 1mb
		pathPrefix:     src.PathPrefix,
		pathPattern:    src.PathPattern,
		hideSystemCols: src.HideSystemCols,
		unparsedPolicy: src.UnparsedPolicy,
	}, nil
}
