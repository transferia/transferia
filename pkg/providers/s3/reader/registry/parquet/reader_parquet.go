package parquet

import (
	"bytes"
	"context"
	"time"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ s3_reader.S3Reader = (*ReaderParquet)(nil)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatPARQUET, NewParquetReader, NewParquetSchemaResolver)
}

type ReaderParquet struct {
	s3RawReaderBuilder s3raw.S3RawReaderBuilder
	table              abstract.TableID
	bucket             string
	client             s3iface.S3API
	logger             log.Logger
	hideSystemCols     bool
	batchSize          int
	pathPrefix         string
	pathPattern        string
	metrics            *stats.SourceStats
	s3RawReader        s3raw.S3RawReader
	unparsedPolicy     s3_model.UnparsedPolicy
}

func (r *ReaderParquet) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	meta, err := r.newS3RawReader(ctx, *obj.Key)
	if err != nil {
		return 0, xerrors.Errorf("could not create reader for object %s, err: %w", *obj.Key, err)
	}
	defer meta.Close()

	return uint64(meta.NumRows()), nil
}

func (r *ReaderParquet) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	result := uint64(0)

	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, s3_reader.IsNotEmpty)
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list, err: %w", err)
	}

	// we take only part of found files - to calculate their rows count
	limit := s3_reader.EstimateFilesLimit
	processed := 0
	for _, file := range files {
		if processed >= limit {
			break
		}

		meta, err := r.newS3RawReader(ctx, *file.Key)
		if err != nil {
			return 0, xerrors.Errorf("unable to read file meta: %s, err: %w", *file.Key, err)
		}

		result += uint64(meta.NumRows())
		_ = meta.Close()

		processed++
	}

	// we need all matches files list here
	if len(files) > limit {
		multiplier := float64(len(files)) / float64(limit)
		return uint64(float64(result) * multiplier), nil
	}

	return result, nil
}

// newS3RawReader
// openParquetFromReaderClassifiesParquetPanics returns DataFile (not a bare panic) when parquet-go
// cannot open a corrupt/invalid file (see base.md Parquet corrupt-file contract).
// Touch metadata so corrupt footers become errors/panics here instead of in Read().
func (r *ReaderParquet) newS3RawReader(ctx context.Context, filePath string) (*parquet.Reader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(r.s3RawReaderBuilder).BuildReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to build reader, err: %w", err)
	}
	r.s3RawReader = s3RawReader

	// For compressed files, parquet-go requires correct Size() which should return uncompressed size.
	// wrappedReader.Size() returns compressed size from S3 metadata, which breaks footer reading.
	// Solution: if reader implements ReaderAll (i.e., it's a compressed file wrapper),
	// load the full uncompressed content and create bytes.Reader.
	if readerAll, ok := s3RawReader.(s3raw.ReaderAll); ok {
		data, err := readerAll.ReadAll(ctx)
		if err != nil {
			return nil, xerrors.Errorf("unable to read all, filePath: %s, err: %w", filePath, err)
		}
		result, err := openParquetFromReaderClassifiesParquetPanics(filePath, bytes.NewReader(data))
		if err != nil {
			return nil, xerrors.Errorf(
				"parquet.newS3RawReader.open: %w",
				reader_error.NewReaderErrorDataFile("parquet.newS3RawReader.open", filePath, err),
			)
		}
		return result, nil
	}

	result, err := openParquetFromReaderClassifiesParquetPanics(filePath, s3RawReader)
	if err != nil {
		return nil, xerrors.Errorf(
			"parquet.newS3RawReader.open: %w",
			reader_error.NewReaderErrorDataFile("parquet.newS3RawReader.open", filePath, err),
		)
	}
	return result, nil
}

//nolint:descriptiveerrors
func (r *ReaderParquet) Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	if schema == nil || len(schema.Columns()) == 0 {
		return nil
	}
	colNames := yslices.Map(schema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })

	pr, err2 := r.newS3RawReader(ctx, filePath)
	if err2 != nil {
		if handled, res := s3_reader.TryHandleDataErrorWithFlush(
			ctx,
			r.table,
			r.unparsedPolicy,
			filePath,
			0,
			0,
			err2,
			pusher,
			func(e reader_error.ReaderError) reader_error.ReaderError {
				return wrapParquetReadFlushChunk("parquet.Read.newS3RawReader.FlushChunk", e)
			},
		); handled {
			return res
		}
		return reader_error.NewReaderErrorFromObjectOpen("parquet.parser.Read.newS3RawReader", filePath, err2)
	}
	defer pr.Close()

	rowCount := uint64(pr.NumRows())
	r.logger.Infof("part: %s extracted row count: %v", filePath, rowCount)
	var buff []abstract.ChangeItem

	rowFields := make(map[string]parquet.Field)
	for _, field := range pr.Schema().Fields() {
		rowFields[field.Name()] = field
	}
	r.logger.Infof("schema: \n%s", pr.Schema())

	var currentSize int64
	for i := uint64(0); i < rowCount; {
		if ctx.Err() != nil {
			r.logger.Info("Read canceled")
			return nil
		}
		row := make(map[string]any)
		if err := pr.Read(&row); err != nil {
			dataErr := reader_error.NewReaderErrorDataFile("parquet.ReadRow", filePath, err)
			if handled, res := s3_reader.TryHandleDataErrorWithFlush(
				ctx,
				r.table,
				r.unparsedPolicy,
				filePath,
				i,
				currentSize,
				dataErr,
				pusher,
				func(e reader_error.ReaderError) reader_error.ReaderError {
					return wrapParquetReadFlushChunk("parquet.Read.FlushChunk.rowErr", e)
				},
			); handled {
				return res
			}
		}
		i += 1
		changeItem, err := r.constructCI(rowFields, row, filePath, r.s3RawReader.LastModified(), i, schema, colNames)
		if err != nil {
			errorData := reader_error.NewReaderErrorDataRecord("parquet.constructCI", filePath, i, err)
			changeItems, handleErr := reader_error.HandleDataError(r.table, r.unparsedPolicy, errorData)
			if handleErr != nil {
				return handleErr
			}
			if len(changeItems) != 0 {
				for _, currChangeItem := range changeItems {
					currentSize += int64(currChangeItem.Size.Values)
					buff = append(buff, currChangeItem)
				}
			}
		} else {
			currentSize += int64(changeItem.Size.Values)
			buff = append(buff, changeItem)
		}
		if len(buff) > r.batchSize {
			if err := s3_reader.FlushChunk(ctx, filePath, i, currentSize, buff, pusher); err != nil {
				return wrapParquetReadFlushChunk("parquet.Read.FlushChunk.batch", err)
			}
			currentSize = 0
			buff = make([]abstract.ChangeItem, 0)
		}
	}
	if err := s3_reader.FlushChunk(ctx, filePath, rowCount, currentSize, buff, pusher); err != nil {
		return wrapParquetReadFlushChunk("parquet.Read.FlushChunk.final", err)
	}

	return nil
}

func (r *ReaderParquet) constructCI(
	parquetSchema map[string]parquet.Field,
	row map[string]any,
	fileName string,
	lModified time.Time,
	idx uint64,
	schema *abstract.TableSchema,
	colNames []string,
) (abstract.ChangeItem, error) {
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
				vals[i] = idx
			default:
				continue
			}
			continue
		}
		val, ok := row[col.ColumnName]
		if !ok {
			vals[i] = nil
		} else {
			vals[i] = r.parseParquetField(parquetSchema[col.ColumnName], val, col)
		}
	}

	return abstract.ChangeItem{
		ID:               0,
		LSN:              0,
		CommitTime:       uint64(lModified.UnixNano()),
		Counter:          int(idx),
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

func (r *ReaderParquet) parseLogicalDate(field parquet.Field, val any) any {
	switch {
	case field.Type().LogicalType().Date != nil:
		switch v := val.(type) {
		case int32:
			// handle logical int32 variations:
			if field.Type().LogicalType().Date != nil {
				return time.Unix(0, 0).Add(24 * time.Duration(v) * time.Hour)
			}
		}
	}
	return val
}

func (r *ReaderParquet) parseParquetField(field parquet.Field, val any, col abstract.ColSchema) any {
	if field == nil || field.Type() == nil {
		return val
	}
	if legacyInt96, ok := val.(deprecated.Int96); ok {
		return legacyInt96.String()
	}
	if field.Type().LogicalType() != nil {
		switch {
		case field.Type().LogicalType().Date != nil:
			return r.parseLogicalDate(field, val)
		}
	}
	if field.Type().ConvertedType() != nil {
		switch *field.Type().ConvertedType() {
		case deprecated.Date:
			return r.parseLogicalDate(field, val)
		}
	}
	return abstract.Restore(col, val)
}

func NewParquetReader(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3Reader, error) {
	if src == nil {
		return nil, xerrors.New("parquet.NewParquet.uninitialized")
	}
	return &ReaderParquet{
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
		pathPattern:    src.PathPattern,

		metrics:        metrics,
		s3RawReader:    nil,
		unparsedPolicy: src.UnparsedPolicy,
	}, nil
}
