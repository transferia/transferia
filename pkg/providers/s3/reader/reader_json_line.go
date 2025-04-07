package reader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/goccy/go-json"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/parsers/scanner"
	"github.com/transferia/transferia/pkg/providers/s3"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/valyala/fastjson"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	_              Reader     = (*JSONLineReader)(nil)
	_              RowCounter = (*JSONLineReader)(nil)
	RestColumnName            = "rest"
)

type JSONLineReader struct {
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
	pathPrefix              string
	newlinesInValue         bool
	unexpectedFieldBehavior s3.UnexpectedFieldBehavior
	blockSize               int64
	pathPattern             string
	metrics                 *stats.SourceStats
	unparsedPolicy          s3.UnparsedPolicy
}

func (r *JSONLineReader) openReader(ctx context.Context, filePath string) (*S3Reader, error) {
	sr, err := NewS3Reader(ctx, r.client, r.downloader, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader at: %w", err)
	}
	return sr, nil
}

func (r *JSONLineReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	res := uint64(0)

	totalSize, sampleReader, err := estimateTotalSize(ctx, r.logger, files, r.openReader)
	if err != nil {
		return 0, xerrors.Errorf("unable to estimate rows: %w", err)
	}

	if totalSize > 0 && sampleReader != nil {
		data := make([]byte, r.blockSize)
		bytesRead, err := sampleReader.ReadAt(data, 0)
		if err != nil && !xerrors.Is(err, io.EOF) {
			return uint64(0), xerrors.Errorf("failed to estimate row count: %w", err)
		}
		if bytesRead > 0 {
			lines, bytesRead := readAllMultilineLines(data)
			bytesPerLine := float64(bytesRead) / float64(len(lines))
			totalLines := math.Ceil(float64(totalSize) / bytesPerLine)
			res = uint64(totalLines)
		}
	}
	return res, nil
}

func (r *JSONLineReader) RowCount(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s : %w", *obj.Key, err)
	}
	return res, nil
}

func (r *JSONLineReader) TotalRowCount(ctx context.Context) (uint64, error) {
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

func (r *JSONLineReader) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
	s3Reader, err := r.openReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open reader: %w", err)
	}

	offset := 0
	lineCounter := uint64(1)
	var readBytes int
	var lines []string

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Read canceled")
			return nil
		default:
		}
		data := make([]byte, r.blockSize)
		lastRound := false
		n, err := s3Reader.ReadAt(data, int64(offset))
		if err != nil {
			if xerrors.Is(err, io.EOF) && n > 0 {
				data = data[0:n]
				lastRound = true
			} else {
				return xerrors.Errorf("failed to read from file: %w", err)
			}
		}
		if r.newlinesInValue {
			lines, readBytes = readAllMultilineLines(data)
		} else {
			lines, readBytes, err = readAllLines(data)
			if err != nil {
				return xerrors.Errorf("failed to read lines from file: %w", err)
			}
		}

		offset += readBytes
		var buff []abstract.ChangeItem
		var currentSize int64
		for _, line := range lines {
			ci, err := r.doParse(line, filePath, s3Reader.LastModified(), lineCounter)
			if err != nil {
				unparsedCI, err := handleParseError(r.table, r.unparsedPolicy, filePath, int(lineCounter), err)
				if err != nil {
					return err
				}
				buff = append(buff, *unparsedCI)
				continue
			}
			currentSize += int64(ci.Size.Values)
			lineCounter++
			buff = append(buff, *ci)
			if len(buff) > r.batchSize {
				if err := pusher.Push(ctx, chunk_pusher.Chunk{
					Items:     buff,
					FilePath:  filePath,
					Offset:    lineCounter,
					Completed: lastRound,
					Size:      currentSize,
				}); err != nil {
					return xerrors.Errorf("unable to push: %w", err)
				}
				currentSize = 0
				buff = []abstract.ChangeItem{}
			}
		}
		if len(buff) > 0 {
			if err := pusher.Push(ctx, chunk_pusher.Chunk{
				Items:     buff,
				FilePath:  filePath,
				Offset:    lineCounter,
				Completed: lastRound,
				Size:      currentSize,
			}); err != nil {
				return xerrors.Errorf("unable to push: %w", err)
			}
		}
		if lastRound {
			break
		}
	}

	return nil
}

func (r *JSONLineReader) doParse(line string, filePath string, lastModified time.Time, lineCounter uint64) (*abstract.ChangeItem, error) {
	row := make(map[string]any)
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal json line: %w", err)
	}

	ci, err := r.constructCI(row, filePath, lastModified, lineCounter)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct change item: %w", err)
	}

	if err := strictify.Strictify(ci, r.fastCols); err != nil {
		return nil, xerrors.Errorf("failed to convert value to the expected data type: %w", err)
	}
	return ci, nil
}

func (r *JSONLineReader) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	// the most complex and useful method in the world
	return chunk.Items
}

func (r *JSONLineReader) constructCI(row map[string]any, fname string, lastModified time.Time, idx uint64) (*abstract.ChangeItem, error) {
	vals := make([]interface{}, len(r.tableSchema.Columns()))
	rest := make(map[string]any)
	for key, val := range row {
		known := false
		for _, col := range r.tableSchema.Columns() {
			if col.ColumnName == key {
				known = true
				break
			}
		}
		if !known {
			if r.unexpectedFieldBehavior == s3.Infer {
				rest[key] = val
			} else if r.unexpectedFieldBehavior == s3.Ignore {
				continue
			} else {
				return nil, xerrors.NewSentinel("unexpected json field found in jsonline file")
			}
		}
	}
	// TODO: add support for col.Path
	for i, col := range r.tableSchema.Columns() {
		if col.PrimaryKey {
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
		val, ok := row[col.ColumnName]
		if !ok {
			if col.ColumnName == RestColumnName && r.unexpectedFieldBehavior == s3.Infer {
				vals[i] = abstract.Restore(col, rest)
			} else {
				vals[i] = nil
			}
			continue
		}
		vals[i] = val
	}

	return &abstract.ChangeItem{
		CommitTime:   uint64(lastModified.UnixNano()),
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

func (r *JSONLineReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	if r.tableSchema != nil && len(r.tableSchema.Columns()) != 0 {
		return r.tableSchema, nil
	}

	files, err := ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, aws.Int(1), r.ObjectsFilter())
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	if len(files) < 1 {
		return nil, xerrors.Errorf("unable to resolve schema, no jsonline files found: %s", r.pathPrefix)
	}

	return r.resolveSchema(ctx, *files[0].Key)
}

func (r *JSONLineReader) ObjectsFilter() ObjectsFilter { return IsNotEmpty }

func (r *JSONLineReader) resolveSchema(ctx context.Context, key string) (*abstract.TableSchema, error) {
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

	reader := bufio.NewReader(bytes.NewReader(buff))
	var line string
	if r.newlinesInValue {
		line, err = readSingleJSONObject(reader)
		if err != nil {
			return nil, xerrors.Errorf("could not read sample data with newlines for schema deduction from %s: %w", r.pathPrefix+key, err)
		}
	} else {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, xerrors.Errorf("could not read sample data for schema deduction from %s: %w", r.pathPrefix+key, err)
		}
	}

	if err := fastjson.Validate(line); err != nil {
		return nil, xerrors.Errorf("failed to validate json line from %s: %w", r.pathPrefix+key, err)
	}

	unmarshaledJSONLine := make(map[string]interface{})
	if err := json.Unmarshal([]byte(line), &unmarshaledJSONLine); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal json line from %s: %w", r.pathPrefix+key, err)
	}

	keys := util.MapKeysInOrder(unmarshaledJSONLine)
	var cols []abstract.ColSchema

	for _, key := range keys {
		val := unmarshaledJSONLine[key]
		if val == nil {
			col := abstract.NewColSchema(key, schema.TypeAny, false)
			col.OriginalType = fmt.Sprintf("jsonl:%s", "null")
			cols = append(cols, col)
			continue
		}

		valueType, originalType, err := guessType(val)
		if err != nil {
			return nil, xerrors.Errorf("failed to guess schema type for field %s from %s: %w", key, r.pathPrefix+key, err)
		}

		col := abstract.NewColSchema(key, valueType, false)
		col.OriginalType = fmt.Sprintf("jsonl:%s", originalType)
		cols = append(cols, col)
	}

	if r.unexpectedFieldBehavior == s3.Infer {
		restCol := abstract.NewColSchema(RestColumnName, schema.TypeAny, false)
		restCol.OriginalType = fmt.Sprintf("jsonl:%s", "string")
		cols = append(cols, restCol)
	}

	return abstract.NewTableSchema(cols), nil
}

func guessType(value interface{}) (schema.Type, string, error) {
	switch result := value.(type) {
	case map[string]interface{}:
		// is object so any
		return schema.TypeAny, "object", nil
	case []interface{}:
		// is array so any
		return schema.TypeAny, "array", nil
	case string:
		if _, err := cast.ToTimeE(result); err == nil {
			return schema.TypeTimestamp, "timestamp", nil
		}
		return schema.TypeString, "string", nil
	case bool:
		return schema.TypeBoolean, "boolean", nil
	case float64:
		return schema.TypeFloat64, "number", nil
	default:
		return schema.TypeAny, "", xerrors.Errorf("unknown json type")
	}
}

func readAllLines(content []byte) ([]string, int, error) {
	scanner := scanner.NewLineBreakScanner(content)
	scannedLines, err := scanner.ScanAll()
	if err != nil {
		return nil, 0, xerrors.Errorf("failed to split all read lines: %w", err)
	}

	var lines []string

	bytesRead := 0
	for index, line := range scannedLines {
		if index == len(scannedLines)-1 {
			// check if last line is complete
			if err := fastjson.Validate(string(line)); err != nil {
				break
			}
		}
		lines = append(lines, string(line))
		bytesRead += (len(line) + len("\n"))
	}
	return lines, bytesRead, nil
}

// In order to comply with the POSIX standard definition of line https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_206
func readAllMultilineLines(content []byte) ([]string, int) {
	var lines []string
	extractedLine := []rune{}
	foundStart := false
	countCurlyBrackets := 0
	bytesRead := 0
	for _, char := range string(content) {
		if foundStart && countCurlyBrackets == 0 {
			lines = append(lines, string(extractedLine))
			bytesRead += (len(string(extractedLine)) + len("\n"))

			foundStart = false
			countCurlyBrackets = 0
			extractedLine = []rune{}
			continue
		}
		extractedLine = append(extractedLine, char)
		if char == '{' {
			countCurlyBrackets++
			foundStart = true
			continue
		}

		if char == '}' {
			countCurlyBrackets--
		}
	}
	return lines, bytesRead
}

func readSingleJSONObject(reader *bufio.Reader) (string, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return "", xerrors.Errorf("failed to read sample content for schema deduction: %w", err)
	}

	extractedLine := []rune{}
	foundStart := false
	countCurlyBrackets := 0
	for _, char := range string(content) {
		if foundStart && countCurlyBrackets == 0 {
			break
		}

		extractedLine = append(extractedLine, char)
		if char == '{' {
			countCurlyBrackets++
			foundStart = true
			continue
		}

		if char == '}' {
			countCurlyBrackets--
		}
	}
	return string(extractedLine), nil
}

func NewJSONLineReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (*JSONLineReader, error) {
	if src == nil || src.Format.JSONLSetting == nil {
		return nil, xerrors.New("uninitialized settings for jsonline reader")
	}

	jsonlSettings := src.Format.JSONLSetting

	reader := &JSONLineReader{
		bucket:                  src.Bucket,
		hideSystemCols:          src.HideSystemCols,
		batchSize:               src.ReadBatchSize,
		pathPrefix:              src.PathPrefix,
		pathPattern:             src.PathPattern,
		newlinesInValue:         jsonlSettings.NewlinesInValue,
		unexpectedFieldBehavior: jsonlSettings.UnexpectedFieldBehavior,
		blockSize:               jsonlSettings.BlockSize,
		client:                  aws_s3.New(sess),
		downloader:              s3manager.NewDownloader(sess),
		logger:                  lgr,
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		tableSchema:    abstract.NewTableSchema(src.OutputSchema),
		fastCols:       abstract.NewTableSchema(src.OutputSchema).FastColumns(),
		colNames:       nil,
		metrics:        metrics,
		unparsedPolicy: src.UnparsedPolicy,
	}

	if len(reader.tableSchema.Columns()) == 0 {
		var err error
		reader.tableSchema, err = reader.ResolveSchema(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve schema: %w", err)
		}
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
