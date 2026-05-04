package csv

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/abstract/model"
	dt_csv "github.com/transferia/transferia/pkg/csv"
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
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var _ s3_reader.S3Reader = (*CSVReader)(nil)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatCSV, NewCSVReader, NewCSVSchemaResolver)
}

// CSVReader reads CSV objects; schema comes from ResolveSchema via ReaderContractor (not stored here).
type CSVReader struct {
	config         csvConfig
	table          abstract.TableID
	unparsedPolicy s3_model.UnparsedPolicy
	maxBatchSize   int // from s3 file read buf-by-buf, into every buf read by #maxBatchSize amount of changeItems
}

func (r *CSVReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	readAllImpl := func(in []byte) ([]string, uint64, error) {
		csvReader := r.config.newCSVReaderFromReader(bufio.NewReader(bytes.NewReader(in)))
		rowsAndColumns, err := csvReader.ReadAll()
		if err != nil {
			return nil, 0, xerrors.Errorf("unable to read all csv lines, err: %w", err)
		}
		bytesRead := csvReader.GetOffset()
		resultLines := make([]string, len(rowsAndColumns))
		return resultLines, uint64(bytesRead), nil
	}
	return estimators.EstimateRows(ctx, r.config.logger, files, int(r.config.blockSize), r.config.newS3RawReader, readAllImpl)
}

func (r *CSVReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("unable to estimate rows count, key: %s, err: %w", *obj.Key, err)
	}
	return res, nil
}

func (r *CSVReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.config.bucket, r.config.pathPrefix, r.config.pathPattern, r.config.client, r.config.logger, nil, s3_reader.IsNotEmpty)
	if err != nil {
		return 0, xerrors.Errorf("unable to list files, err: %w", err)
	}

	res, err := r.estimateRows(ctx, files)
	if err != nil {
		return 0, xerrors.Errorf("unable to estimateRows, err: %w", err)
	}
	return res, nil
}

//nolint:descriptiveerrors
func (r *CSVReader) Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	if schema == nil || len(schema.Columns()) == 0 {
		return nil
	}
	fastCols := schema.FastColumns()
	colNames := yslices.Map(schema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })

	s3RawReader, err := r.config.newS3RawReader(ctx, filePath)
	if err != nil {
		return reader_error.NewReaderErrorTransport("csv.parser.Read.newS3RawReader", r.config.pathPrefix, err)
	}

	offsetInFile := int64(0) // offset from beginning of file!
	rowsCounter := uint64(1)
	chunkReader := s3_reader.NewChunkReader(s3RawReader, s3_reader.DefaultChunkReaderBlockSize, r.config.logger)
	defer chunkReader.Close()

	for { // this loop - over one file, read buffer-by-buffer
		if ctx.Err() != nil {
			r.config.logger.Info("Read canceled")
			return nil
		}

		csvReader, endOfFileReached, err2 := r.readBufferFromChunkReader(chunkReader, offsetInFile, filePath)
		if err2 != nil {
			if handled, res := s3_reader.TryHandleDataErrorWithFlush(ctx, r.table, r.unparsedPolicy, filePath, rowsCounter, 0, err2, pusher, wrapCSVReadFlushChunk); handled {
				return res
			}
			return reader_error.WrapReaderError("csv.Read.readBufferFromChunkReader", err2)
		}

		offsetInBuf := int64(0)
		for { // this loop - into this one buffer, which we just read from s3, parse batch-by-batch
			offsetInBufBefore := csvReader.GetOffset()

			changeItems, err3 := r.parseCSVRows(csvReader, filePath, s3RawReader.LastModified(), &rowsCounter, r.maxBatchSize, schema, fastCols, colNames)
			if err3 != nil {
				if handled, res := s3_reader.TryHandleDataErrorWithFlush(ctx, r.table, r.unparsedPolicy, filePath, rowsCounter, 0, err3, pusher, wrapCSVReadFlushChunk); handled {
					if res != nil {
						return res
					}
					if r.unparsedPolicy == s3_model.UnparsedPolicyContinue {
						return nil
					}
					return reader_error.WrapReaderError("csv.Read.parseCSVRows", err3)
				}
				return reader_error.WrapReaderError("csv.Read.parseCSVRows", err3)
			}

			offsetInBuf = csvReader.GetOffset()
			parsedSize := offsetInBuf - offsetInBufBefore

			if len(changeItems) == 0 {
				break
			}

			if err := s3_reader.FlushChunk(ctx, filePath, rowsCounter, parsedSize, changeItems, pusher); err != nil {
				return wrapCSVReadFlushChunk(err)
			}
		}

		chunkReader.FillBuffer(chunkReader.Data()[offsetInBuf:])
		offsetInFile += offsetInBuf
		if endOfFileReached {
			break
		}
	}
	return nil
}

// readBufferFromChunkReader reads range [offset + blockSize] from S3 bucket.
// It returns a *csv.Reader that should be used for csv rows reading.
// It returns a boolean flag indicating whether the end of the S3 file was reached.
// It returns any error it encounters during the reading process.
//
//nolint:descriptiveerrors
func (r *CSVReader) readBufferFromChunkReader(
	chunkReader *s3_reader.ChunkReader,
	offsetInFile int64,
	filePath string,
) (*dt_csv.Reader, bool, reader_error.ReaderError) {
	if err := chunkReader.ReadNextChunk(); err != nil {
		if !xerrors.Is(err, io.EOF) {
			return nil, false, reader_error.NewReaderErrorTransport("chunk.ReadNextChunk", filePath, err)
		}
	}

	csvReader := r.config.newCSVReaderFromReader(bytes.NewReader(chunkReader.Data()))
	if offsetInFile == 0 {
		if err := r.skipUnnecessaryLines(csvReader, filePath); err != nil {
			return nil, chunkReader.IsEOF(), reader_error.NewReaderErrorDataSchema("csv.skipUnnecessaryLines", filePath, err)
		}
	}

	return csvReader, chunkReader.IsEOF(), nil
}

// parseCSVRows reads and parses line by line the fetched data block from S3.
// If EOF or maxBatchSize limit is reached the extracted changeItems are returned.
//
//nolint:descriptiveerrors
func (r *CSVReader) parseCSVRows(
	csvReader *dt_csv.Reader,
	filePath string,
	lastModified time.Time,
	rowNumber *uint64,
	maxBatchSize int,
	schema *abstract.TableSchema,
	fastCols abstract.FastTableSchema,
	colNames []string,
) ([]abstract.ChangeItem, reader_error.ReaderError) {
	var result []abstract.ChangeItem
	for {
		line, err := csvReader.ReadLine()
		if xerrors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return nil, reader_error.NewReaderErrorDataRecord("csv.ReadLine", filePath, *rowNumber, err)
		}

		changeItem, err := r.doParse(line, filePath, lastModified, *rowNumber, schema, fastCols, colNames)
		if err != nil {
			errorData, ok := reader_error.AsReaderErrorData(err)
			if !ok {
				errorData = reader_error.NewReaderErrorDataRecord("csv.doParse", filePath, *rowNumber, err)
			}
			items, handleErr := reader_error.HandleDataError(r.table, r.unparsedPolicy, errorData)
			if handleErr != nil {
				return nil, handleErr
			}
			result = append(result, items...)
			*rowNumber++
			continue
		}
		*rowNumber += 1

		result = append(result, *changeItem)

		if len(result) > maxBatchSize {
			return result, nil
		}
	}
}

func (r *CSVReader) doParse(
	line []string,
	filePath string,
	lastModified time.Time,
	rowNumber uint64,
	schema *abstract.TableSchema,
	fastCols abstract.FastTableSchema,
	colNames []string,
) (*abstract.ChangeItem, error) {
	changeItem, err := r.constructCI(line, filePath, lastModified, rowNumber, schema, colNames)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct changeItem, err: %w", err)
	}
	if err := strictify.Strictify(changeItem, fastCols); err != nil {
		return nil, xerrors.Errorf("failed to strictify, err: %w", err)
	}
	return changeItem, nil
}

// skipUnnecessaryLines skips the lines before the actual csv content starts.
// This might include lines before the header line, the header line itself and possible lines after the header.
// The amount of lines to skip is passed by the user in the SkipRows and SkipRowsAfterNames parameter.
func (r *CSVReader) skipUnnecessaryLines(csvReader *dt_csv.Reader, fileKey string) error {
	if err := skipRows(r.config.advancedOptions.SkipRows, csvReader, fileKey); err != nil {
		return xerrors.Errorf("unable to skip rows 1, err: %w", err)
	}

	if r.config.headerPresent {
		if err := skipRows(r.config.advancedOptions.SkipRowsAfterNames+1, csvReader, fileKey); err != nil {
			return xerrors.Errorf("unable to skip rows 2, err: %w", err)
		}
	}
	return nil
}

//nolint:descriptiveerrors
func (r *CSVReader) constructCI(
	row []string,
	fileName string,
	lModified time.Time,
	rowNumber uint64,
	schema *abstract.TableSchema,
	colNames []string,
) (*abstract.ChangeItem, reader_error.ReaderError) {
	vals := make([]any, len(schema.Columns()))
	for i, col := range schema.Columns() {
		if s3_reader.SystemColumnNames[col.ColumnName] {
			if r.config.hideSystemCols {
				vals[i] = nil
				continue
			}
			switch col.ColumnName {
			case s3_reader.FileNameSystemCol:
				vals[i] = fileName
			case s3_reader.RowIndexSystemCol:
				vals[i] = rowNumber
			default:
				continue
			}
			continue
		}

		index, err := strconv.Atoi(col.Path)
		if err != nil {
			return nil, reader_error.NewReaderErrorDataRecord("csv.constructCI.colPath", fileName, rowNumber, err)
		}
		if index < 0 {
			vals[i] = abstract.DefaultValue(&col)
		} else {
			if index >= len(row) {
				if r.config.additionalReaderOptions.IncludeMissingColumns {
					vals[i] = abstract.DefaultValue(&col)
				} else {
					return nil, reader_error.NewReaderErrorDataRecord(
						"csv.constructCI.missing_cell",
						fileName,
						rowNumber,
						fmt.Errorf(
							"missing row element for column: %s, row elements: %d, columns: %d",
							col.ColumnName,
							len(row),
							len(vals),
						),
					)
				}
			} else {
				originalValue := row[index]
				val := r.getCorrespondingValue(originalValue, col)
				vals[i] = val
			}
		}
	}

	return &abstract.ChangeItem{
		ID:               0,
		LSN:              0,
		CommitTime:       uint64(lModified.UnixNano()),
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

// getCorrespondingValue performs a check/transformation with the original value against the user provided configuration
// such as: NullValues, TrueValues, FalseValues, TimestampParsers, DecimalPoint, to derive its corresponding value.
func (r *CSVReader) getCorrespondingValue(originalValue string, col abstract.ColSchema) any {
	var resultingValue any

	switch col.DataType {
	case ytschema.TypeBoolean.String():
		resultingValue = r.parseBooleanValue(originalValue)
	case ytschema.TypeDate.String(), ytschema.TypeDatetime.String():
		resultingValue = r.parseDateValue(originalValue)
	case ytschema.TypeTimestamp.String():
		resultingValue = r.parseTimestampValue(originalValue)
	case ytschema.TypeFloat32.String(), ytschema.TypeFloat64.String():
		resultingValue = r.parseFloatValue(originalValue)
	default:
		resultingValue = r.parseNullValues(originalValue, col)
	}

	return resultingValue
}

// parseFloatValue checks if the provided value can be correctly parsed to a float value
// if the specified decimal char is used.
// It defaults to the value itself if this conversion is not possible.
func (r *CSVReader) parseFloatValue(originalValue string) any {
	if r.config.additionalReaderOptions.DecimalPoint != "" {
		possibleFloat := strings.Replace(originalValue, r.config.additionalReaderOptions.DecimalPoint, ".", 1)
		_, err := strconv.ParseFloat(possibleFloat, 64)
		if err == nil {
			return possibleFloat
		} else {
			return originalValue
		}

	} else {
		return originalValue
	}
}

// parseNullValues checks if the provided value is part of the null values list provided by the user.
// If this is the case the zero value of the datatype is returned for this value.
// It defaults to the original value if the value is not contained in the list or if the  conditions of the
// boolean flags (QuotedStringsCanBeNull, StringsCanBeNull) are not fulfilled.
func (r *CSVReader) parseNullValues(originalValue string, col abstract.ColSchema) any {
	if r.config.additionalReaderOptions.QuotedStringsCanBeNull {
		trimmedContent := originalValue
		if strings.HasPrefix(originalValue, "\"") && strings.HasSuffix(originalValue, "\"") {
			trimmedContent = strings.TrimSuffix(strings.TrimPrefix(originalValue, "\""), "\"")
		} else if strings.HasPrefix(originalValue, "'") && strings.HasSuffix(originalValue, "'") {
			trimmedContent = strings.TrimSuffix(strings.TrimPrefix(originalValue, "'"), "'")
		}
		if yslices.Contains(r.config.additionalReaderOptions.NullValues, trimmedContent) {
			return abstract.DefaultValue(&col)
		}
	} else {
		if r.config.additionalReaderOptions.StringsCanBeNull {
			if yslices.Contains(r.config.additionalReaderOptions.NullValues, originalValue) {
				return abstract.DefaultValue(&col)
			}
		}
	}

	return originalValue
}

// parseDateValue checks if the provided value can be parsed to a time.Time through one of
// the user provided TimestampParsers. It defaults to the original value if this is not the case.
func (r *CSVReader) parseDateValue(originalValue string) any {
	for _, parser := range r.config.additionalReaderOptions.TimestampParsers {
		dateValue, err := time.Parse(parser, originalValue)
		if err == nil {
			return dateValue
		}
	}
	return originalValue
}

// parseTimestampValue checks if the provided value can be parsed to a time.Time.
// It defaults to the original value if this is not the case.
func (r *CSVReader) parseTimestampValue(originalValue string) any {
	toInt64, err := strconv.ParseInt(originalValue, 10, 64)
	if err == nil {
		return time.Unix(toInt64, 0)
	}

	return originalValue
}

// parseBooleanValue checks if the provided value is contained in one of the provided lists of
// true/false values and returns the corresponding boolean value. If the value is contained in the null values
// then a false boolean value is returned for this value. It defaults to the original value if no matches are found.
func (r *CSVReader) parseBooleanValue(originalValue string) any {
	if r.config.additionalReaderOptions.StringsCanBeNull {
		if yslices.Contains(r.config.additionalReaderOptions.NullValues, originalValue) {
			return false
		}
	}
	if yslices.Contains(r.config.additionalReaderOptions.TrueValues, originalValue) {
		return true
	} else if yslices.Contains(r.config.additionalReaderOptions.FalseValues, originalValue) {
		return false
	} else {
		// last ditch attempt, try string conversion
		boolVal, err := strconv.ParseBool(originalValue)
		if err != nil {
			return originalValue
		}
		return boolVal
	}
}

func NewCSVReader(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3Reader, error) {
	config, _, err := newCSVConfigFromSource(src, lgr, sess, s3RawReaderBuilder, metrics)
	if err != nil {
		return nil, err
	}
	return &CSVReader{
		config: *config,
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		unparsedPolicy: src.UnparsedPolicy,
		maxBatchSize:   src.ReadBatchSize,
	}, nil
}
