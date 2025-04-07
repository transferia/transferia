package reader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/csv"
	"github.com/transferia/transferia/pkg/providers/s3"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/valyala/fastjson"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	_ Reader     = (*CSVReader)(nil)
	_ RowCounter = (*CSVReader)(nil)
)

type CSVReader struct {
	table                   abstract.TableID
	bucket                  string
	client                  s3iface.S3API
	downloader              *s3manager.Downloader
	logger                  log.Logger
	tableSchema             *abstract.TableSchema
	fastCols                abstract.FastTableSchema
	colNames                []string
	hideSystemCols          bool
	batchSize               int
	blockSize               int64
	pathPrefix              string
	delimiter               rune
	quoteChar               rune
	escapeChar              rune
	encoding                string
	doubleQuote             bool
	newlinesInValue         bool
	additionalReaderOptions s3.AdditionalOptions
	advancedOptions         s3.AdvancedOptions
	headerPresent           bool
	pathPattern             string
	metrics                 *stats.SourceStats
	unparsedPolicy          s3.UnparsedPolicy
}

func (r *CSVReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	if r.tableSchema != nil && len(r.tableSchema.Columns()) != 0 {
		// Resolve schema was already called no need to redo operation, return previous schema
		return r.tableSchema, nil
	}

	files, err := ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, aws.Int(1), r.ObjectsFilter())
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	if len(files) < 1 {
		return nil, xerrors.Errorf("unable to resolve schema, no csv files found: %s", r.pathPrefix)
	}

	return r.resolveSchema(ctx, *files[0].Key)
}

func (r *CSVReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	totalRows := float64(0)

	totalSize, sampleReader, err := estimateTotalSize(ctx, r.logger, files, r.openReader)
	if err != nil {
		return 0, xerrors.Errorf("unable to estimate rows: %w", err)
	}

	if totalSize > 0 && sampleReader != nil {
		data := make([]byte, r.blockSize)
		bytesRead, err := sampleReader.ReadAt(data, 0)
		if err != nil && !xerrors.Is(err, io.EOF) {
			return 0, xerrors.Errorf("failed to estimate row count: %w", err)
		}
		if bytesRead > 0 {
			csvReader := r.newCSVReaderFromReader(bufio.NewReader(bytes.NewReader(data)))
			lines, err := csvReader.ReadAll()
			if err != nil {
				return 0, xerrors.Errorf("failed to read sample lines for row count estimation: %w", err)
			}
			bytesRead := csvReader.GetOffset()
			bytesPerRow := float64(bytesRead) / float64(len(lines))
			totalRows = math.Ceil(float64(totalSize) / bytesPerRow)
		}
	}
	return uint64(totalRows), nil
}

func (r *CSVReader) RowCount(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s : %w", *obj.Key, err)
	}
	return res, nil
}

func (r *CSVReader) TotalRowCount(ctx context.Context) (uint64, error) {
	files, err := ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, r.ObjectsFilter())
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list: %w", err)
	}

	res, err := r.estimateRows(ctx, files)
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate total rows: %w", err)
	}
	return res, nil
}

func (r *CSVReader) openReader(ctx context.Context, filePath string) (*S3Reader, error) {
	sr, err := NewS3Reader(ctx, r.client, r.downloader, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader at: %w", err)
	}
	return sr, nil
}

func (r *CSVReader) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
	s3Reader, err := r.openReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open reader: %w", err)
	}

	offset := int64(0)
	lineCounter := uint64(1)
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Read canceled")
			return nil
		default:
		}

		csvReader, endOfFileReached, err := r.readFromS3(s3Reader, offset)
		if err != nil {
			return xerrors.Errorf("failed to read fom S3 file: %w", err)
		}

		currentOffset := int64(0)
		for {
			buff, err := r.ParseCSVRows(csvReader, filePath, s3Reader.LastModified(), &lineCounter)
			if err != nil {
				return xerrors.Errorf("failed to parse lines from csv file %s: %w", filePath, err)
			}
			currentOffset = csvReader.GetOffset()

			if len(buff) == 0 {
				break
			}
			if err := pusher.Push(ctx, chunk_pusher.Chunk{
				Items:     buff,
				FilePath:  filePath,
				Offset:    lineCounter,
				Completed: endOfFileReached,
				Size:      currentOffset,
			}); err != nil {
				return xerrors.Errorf("unable to push: %w", err)
			}
		}

		offset += currentOffset
		if endOfFileReached {
			break
		}
	}
	return nil
}

// readFromS3 reads range [offset + blockSize] from S3 bucket.
// It returns a *csv.Reader that should be used for csv rows reading.
// It returns a boolean flag if the end of the end of the S3 file was reached.
// It returns any error it encounters during the reading process.
func (r *CSVReader) readFromS3(s3Reader *S3Reader, offset int64) (*csv.Reader, bool, error) {
	data := make([]byte, r.blockSize)
	endOfFile := false
	n, err := s3Reader.ReadAt(data, offset)
	if err != nil {
		if xerrors.Is(err, io.EOF) && n > 0 {
			data = data[0:n]
			endOfFile = true
		} else {
			return nil, endOfFile, xerrors.Errorf("failed to read from file: %w", err)
		}
	}

	csvReader := r.newCSVReaderFromReader(bufio.NewReaderSize(bytes.NewReader(data), 1024))
	if offset == 0 {
		if err := r.skipUnnecessaryLines(csvReader); err != nil {
			return nil, endOfFile, xerrors.Errorf("failed to skip unnecessary rows: %w", err)
		}
	}

	return csvReader, endOfFile, nil
}

// ParseCSVRows reads and parses line by line the fetched data block from S3.
// If EOF or batchSize limit is reached the extracted changeItems are returned.
func (r *CSVReader) ParseCSVRows(csvReader *csv.Reader, filePath string, lastModified time.Time, lineCounter *uint64) ([]abstract.ChangeItem, error) {
	var buff []abstract.ChangeItem
	for {
		line, err := csvReader.ReadLine()
		if xerrors.Is(err, io.EOF) {
			return buff, nil
		}
		if err != nil {
			return nil, xerrors.Errorf("failed to read row form csv: %w", err)
		}

		ci, err := r.doParse(line, filePath, lastModified, *lineCounter)
		if err != nil {
			unparsedCI, err := handleParseError(r.table, r.unparsedPolicy, filePath, int(*lineCounter), err)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse row: %w", err)
			}
			buff = append(buff, *unparsedCI)
			*lineCounter += 1
			continue
		}
		*lineCounter += 1

		buff = append(buff, *ci)

		if len(buff) > r.batchSize {
			return buff, nil
		}
	}
}

func (r *CSVReader) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	// the most complex and useful method in the world
	return chunk.Items
}

func (r *CSVReader) doParse(line []string, filePath string, lastModified time.Time, lineCounter uint64) (*abstract.ChangeItem, error) {
	ci, err := r.constructCI(line, filePath, lastModified, lineCounter)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct change item: %w", err)
	}
	if err := strictify.Strictify(ci, r.fastCols); err != nil {
		return nil, xerrors.Errorf("failed to convert value to the expected data type: %w", err)
	}
	return ci, nil
}

// skipUnnecessaryLines skips the lines before the actual csv content starts.
// This might include lines before the header line, the header line itself and possible lines after the header.
// The amount of lines to skip is passed by the user in the SkipRows and SkipRowsAfterNames parameter.
func (r *CSVReader) skipUnnecessaryLines(csvReader *csv.Reader) error {
	if err := skipRows(r.advancedOptions.SkipRows, csvReader); err != nil {
		return xerrors.Errorf("failed to skip lines from csv file: %w", err)
	}

	if r.headerPresent {
		// skip past header
		if err := skipRows(r.advancedOptions.SkipRowsAfterNames+1, csvReader); err != nil {
			return xerrors.Errorf("failed to skip lines after header from csv file: %w", err)
		}
	}
	return nil
}

func (r *CSVReader) constructCI(row []string, fname string, lModified time.Time, idx uint64) (*abstract.ChangeItem, error) {
	vals := make([]interface{}, len(r.tableSchema.Columns()))
	for i, col := range r.tableSchema.Columns() {
		if systemColumnNames[col.ColumnName] {
			if r.hideSystemCols {
				continue
			}
			switch col.ColumnName {
			case FileNameSystemCol:
				vals[i] = fname
			case RowIndexSystemCol:
				vals[i] = idx
			default:
				continue
			}
			continue
		}

		index, err := strconv.Atoi(col.Path)
		if err != nil {
			return nil, xerrors.Errorf("failed to get index of column: %w", err)
		}
		if index < 0 {
			vals[i] = abstract.DefaultValue(&col)
		} else {
			if index >= len(row) {
				// missing columns should be filled with default value based on data type (if present) or nil by default
				if r.additionalReaderOptions.IncludeMissingColumns {
					vals[i] = abstract.DefaultValue(&col)
				} else {
					return nil, xerrors.Errorf("missing row element for column: %s, row elements: %d, columns: %d",
						col.ColumnName, len(row), len(vals))
				}
			} else {
				originalValue := row[index]
				val := r.getCorrespondingValue(originalValue, col)
				vals[i] = val
			}
		}
	}

	return &abstract.ChangeItem{
		CommitTime:   uint64(lModified.UnixNano()),
		Kind:         abstract.InsertKind,
		Table:        r.table.Name,
		Schema:       r.table.Namespace,
		ColumnNames:  r.colNames,
		ColumnValues: vals,
		TableSchema:  r.tableSchema,
		PartID:       fname,
		ID:           0,
		LSN:          0,
		Counter:      0,
		OldKeys:      abstract.EmptyOldKeys(),
		TxID:         "",
		Query:        "",
		Size:         abstract.RawEventSize(util.DeepSizeof(vals)),
	}, nil
}

func (r *CSVReader) ObjectsFilter() ObjectsFilter { return IsNotEmpty }

func (r *CSVReader) resolveSchema(ctx context.Context, key string) (*abstract.TableSchema, error) {
	s3Reader, err := r.openReader(ctx, key)
	if err != nil {
		return nil, xerrors.Errorf("unable to open reader for file: %s: %w", key, err)
	}

	buff := make([]byte, r.blockSize)
	read, err := s3Reader.ReadAt(buff, 0)
	if err != nil && !xerrors.Is(err, io.EOF) {
		return nil, xerrors.Errorf("failed to read sample from file: %s: %w", key, err)
	}
	if read == 0 {
		// read nothing, file was empty
		return nil, xerrors.New(fmt.Sprintf("could not read sample data from file: %s", key))
	}

	csvReader := r.newCSVReaderFromReader(bytes.NewReader(buff))

	allColNames, err := r.getColumnNames(csvReader)
	if err != nil {
		return nil, xerrors.Errorf("failed to extract column names from csv file: %w", err)
	}

	filteredColNames, err := r.filterColNames(allColNames)
	if err != nil {
		return nil, xerrors.Errorf("failed to filter column names based on additional reader options: %w", err)
	}

	schema, err := r.getColumnTypes(filteredColNames, csvReader)
	if err != nil {
		return nil, xerrors.Errorf("failed to deduce column types based on sample read: %w", err)
	}

	return abstract.NewTableSchema(schema), nil
}

// getColumnTypes deduces the column types for the provided columns.
// Types are inferred based on the read value.
func (r *CSVReader) getColumnTypes(columns []abstract.ColSchema, csvReader *csv.Reader) ([]abstract.ColSchema, error) {
	readAfter := r.advancedOptions.SkipRowsAfterNames
	elements, err := readAfterNRows(readAfter, csvReader)
	if err != nil {
		return nil, xerrors.Errorf("failed to read csv line: %w", err)
	}

	var colsWithSchema []abstract.ColSchema

	for _, col := range columns {
		index, err := strconv.Atoi(col.Path)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse index of column for data type deduction: %w", err)
		}

		// existing column
		if index >= len(elements) {
			// mostly indicates that provided blockSize is to small
			return nil, xerrors.NewSentinel("index of column out of bounds for data type deduction")
		}

		var val string
		if index < 0 {
			val = ""
		} else {
			val = elements[index]
		}

		dataType := r.deduceDataType(val)
		column := abstract.NewColSchema(col.ColumnName, dataType, false)
		column.OriginalType = fmt.Sprintf("csv:%s", dataType.String())
		column.Path = col.Path

		colsWithSchema = append(colsWithSchema,
			column)
	}

	return colsWithSchema, nil
}

// deduceDataType deduces a columns type based on the type more closely matching the read value, if nothing is found it defaults to string data type.
func (r *CSVReader) deduceDataType(val string) schema.Type {
	if val == "" {
		// nothing to deduce from, leave it as string
		return schema.TypeString
	}
	if strings.Contains(val, string('`')) || strings.Contains(val, string('"')) {
		// default of QuotedStringsCanBeNull is true so we need to check that it was not explicitly set to false
		if r.additionalReaderOptions.QuotedStringsCanBeNull {
			if contains(r.additionalReaderOptions.NullValues, val) {
				return schema.TypeString
			}
		}
		// is not a nil type or a date, check if json, else leave as string
		if err := fastjson.Validate(val); err == nil && (strings.Contains(val, "{") || strings.Contains(val, "[")) {
			return schema.TypeAny
		} else {
			return schema.TypeString
		}

	} else {
		if r.additionalReaderOptions.StringsCanBeNull && contains(r.additionalReaderOptions.NullValues, val) {
			return schema.TypeString
		}
		if contains(r.additionalReaderOptions.FalseValues, val) || contains(r.additionalReaderOptions.TrueValues, val) {
			// is boolean
			return schema.TypeBoolean
		}
		if r.additionalReaderOptions.DecimalPoint != "" {
			// we briefly assume its a number
			possibleNumber := strings.Replace(val, r.additionalReaderOptions.DecimalPoint, ".", 1)

			_, err := strconv.ParseFloat(possibleNumber, 64)
			if err == nil {
				return schema.TypeFloat64
			}
		}
		_, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return schema.TypeFloat64
		}

		return schema.TypeString
	}
}

// getColumnNames will extract the column names form the user provided column names.
// If no column names where provided by the user it will check if the names should be autogenerated.
// If both options are not feasible then it will read the first line from file (after skipping N lines as specified by skipRows)
// and use the values read as column names.
func (r *CSVReader) getColumnNames(csvReader *csv.Reader) ([]string, error) {
	var columnNames []string

	if len(r.advancedOptions.ColumnNames) != 0 {
		// column names where provided
		columnNames = append(columnNames, r.advancedOptions.ColumnNames...)
	} else if len(r.advancedOptions.ColumnNames) == 0 && r.advancedOptions.AutogenerateColumnNames {
		// read data after skip_rows to know how many columns to generate
		elements, err := readAfterNRows(r.advancedOptions.SkipRows, csvReader)
		if err != nil {
			return nil, xerrors.Errorf("failed to read csv line after skipping rows: %w", err)
		}
		for i := range elements {
			columnNames = append(columnNames, fmt.Sprintf("f%d", i)) // generate col names
		}
	}

	if len(columnNames) == 0 {
		readAfter := r.advancedOptions.SkipRows
		elements, err := readAfterNRows(readAfter, csvReader)
		if err != nil {
			return nil, xerrors.Errorf("failed to read csv line after skipping rows: %w", err)
		}
		columnNames = append(columnNames, elements...)
	}

	return columnNames, nil
}

// filterColNames filters the required columns based on the values provided by the user in the include_columns parameter.
// If columns not featured in the previously extracted columns are detected in teh include_columns parameter then,
// based on the include_missing_columns its decided if an error should be raised or if a column with null values should be added.
func (r *CSVReader) filterColNames(colNames []string) ([]abstract.ColSchema, error) {
	var cols []abstract.ColSchema
	if len(r.additionalReaderOptions.IncludeColumns) != 0 {
		// only thees columns can be used
		for _, name := range r.additionalReaderOptions.IncludeColumns {
			contained := false
			atIndex := -1
			for index, element := range colNames {
				if element == name {
					contained = true
					atIndex = index
					break
				}
			}

			if !contained && !r.additionalReaderOptions.IncludeMissingColumns {
				// not contained and not allowed to be filled with nil values
				return nil, xerrors.NewSentinel("could not find mandatory column in csv file")
			}
			column := abstract.NewColSchema(name, schema.TypeAny, false)
			column.Path = strconv.Itoa(atIndex)
			cols = append(cols, column)
		}
	} else {
		for index, name := range colNames {
			column := abstract.NewColSchema(name, schema.TypeAny, false)
			column.Path = strconv.Itoa(index)
			cols = append(cols, column)
		}
	}

	return cols, nil
}

// skipRows reads and skips the specified amount of rows.
func skipRows(nrOfRowsToSkip int64, csvReader *csv.Reader) error {
	for i := int64(0); i < nrOfRowsToSkip; i++ {
		// read and ignore lines
		_, err := csvReader.ReadLine()
		if err != nil {
			return xerrors.Errorf("failed to skip csv line: %w", err)
		}
	}
	return nil
}

// readAfterNRows reads and skips the specified amount of csv rows.
// As csv row here a full and complete row is intended (multiline rows are considered as 1 row if so configured).
// It returns the first row read after skipping the specified rows.
func readAfterNRows(nrOfRowsToSkip int64, csvReader *csv.Reader) ([]string, error) {
	if err := skipRows(nrOfRowsToSkip, csvReader); err != nil {
		return nil, xerrors.Errorf("failed to skip %d rows: %w", nrOfRowsToSkip, err)
	}

	elements, err := csvReader.ReadLine()
	if err != nil {
		return nil, xerrors.Errorf("failed to read csv line after %d: %w", nrOfRowsToSkip, err)
	}
	return elements, nil
}

func contains(list []string, element string) bool {
	for _, val := range list {
		if val == element {
			return true
		}
	}

	return false
}

func (r *CSVReader) newCSVReaderFromReader(reader io.Reader) *csv.Reader {
	csvReader := csv.NewReader(reader)
	csvReader.NewlinesInValue = r.newlinesInValue
	csvReader.QuoteChar = r.quoteChar
	csvReader.EscapeChar = r.escapeChar
	csvReader.Encoding = r.encoding
	csvReader.Delimiter = r.delimiter
	csvReader.DoubleQuote = r.doubleQuote
	csvReader.DoubleQuoteStr = fmt.Sprintf("%s%s", string(r.quoteChar), string(r.quoteChar))

	return csvReader
}

func NewCSVReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (*CSVReader, error) {
	if src == nil || src.Format.CSVSetting == nil {
		return nil, xerrors.New("uninitialized settings for csv reader")
	}
	csvSettings := src.Format.CSVSetting

	if len(csvSettings.Delimiter) != 1 {
		return nil, xerrors.Errorf("invalid config, provided delimiter: %s", csvSettings.Delimiter)
	}

	var (
		delimiter  rune
		escapeChar rune
		quoteChar  rune
	)
	if len(csvSettings.Delimiter) > 0 {
		delimiter = []rune(csvSettings.Delimiter)[0]
	}
	if len(csvSettings.QuoteChar) > 0 {
		quoteChar = []rune(csvSettings.QuoteChar)[0]
	}
	if len(csvSettings.EscapeChar) > 0 {
		escapeChar = []rune(csvSettings.EscapeChar)[0]
	}

	reader := &CSVReader{
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:                  src.Bucket,
		client:                  aws_s3.New(sess),
		downloader:              s3manager.NewDownloader(sess),
		logger:                  lgr,
		tableSchema:             abstract.NewTableSchema(src.OutputSchema),
		fastCols:                abstract.NewTableSchema(src.OutputSchema).FastColumns(),
		colNames:                nil,
		hideSystemCols:          src.HideSystemCols,
		batchSize:               src.ReadBatchSize,
		blockSize:               csvSettings.BlockSize,
		pathPrefix:              src.PathPrefix,
		pathPattern:             src.PathPattern,
		delimiter:               delimiter,
		quoteChar:               quoteChar,
		escapeChar:              escapeChar,
		encoding:                csvSettings.Encoding,
		doubleQuote:             csvSettings.DoubleQuote,
		newlinesInValue:         csvSettings.NewlinesInValue,
		additionalReaderOptions: csvSettings.AdditionalReaderOptions,
		advancedOptions:         csvSettings.AdvancedOptions,
		headerPresent:           false,
		metrics:                 metrics,
		unparsedPolicy:          src.UnparsedPolicy,
	}
	if len(reader.tableSchema.Columns()) == 0 {
		if len(reader.advancedOptions.ColumnNames) == 0 && !reader.advancedOptions.AutogenerateColumnNames {
			// header present in csv
			reader.headerPresent = true
		}

		var err error
		reader.tableSchema, err = reader.ResolveSchema(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve schema: %w", err)
		}
	} else {
		// set original types and paths if not set
		var cols []abstract.ColSchema
		for index, col := range reader.tableSchema.Columns() {
			if col.Path == "" {
				col.Path = fmt.Sprintf("%d", index)
			}
			if col.OriginalType == "" {
				col.OriginalType = fmt.Sprintf("csv:%s", col.DataType)
			}
			cols = append(cols, col)
		}
		reader.tableSchema = abstract.NewTableSchema(cols)
	}

	// append system columns at the end if necessary
	if !reader.hideSystemCols {
		cols := reader.tableSchema.Columns()
		reader.tableSchema = appendSystemColsTableSchema(cols)
	}

	reader.colNames = yslices.Map(reader.tableSchema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })
	reader.fastCols = reader.tableSchema.FastColumns() // need to cache it, so we will not construct it for every line
	return reader, nil
}
