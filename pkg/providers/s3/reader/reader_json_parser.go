package reader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/goccy/go-json"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/providers/s3"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/valyala/fastjson"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	_ Reader     = (*JSONParserReader)(nil)
	_ RowCounter = (*JSONParserReader)(nil)
)

type JSONParserReader struct {
	table                   abstract.TableID
	bucket                  string
	client                  s3iface.S3API
	downloader              *s3manager.Downloader
	logger                  log.Logger
	tableSchema             *abstract.TableSchema
	hideSystemCols          bool
	batchSize               int
	pathPrefix              string
	newlinesInValue         bool
	unexpectedFieldBehavior s3.UnexpectedFieldBehavior
	blockSize               int64
	pathPattern             string
	metrics                 *stats.SourceStats
	unparsedPolicy          s3.UnparsedPolicy

	parser parsers.Parser
}

func (r *JSONParserReader) openReader(ctx context.Context, filePath string) (*S3Reader, error) {
	sr, err := NewS3Reader(ctx, r.client, r.downloader, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader at: %w", err)
	}
	return sr, nil
}

func (r *JSONParserReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
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

func (r *JSONParserReader) RowCount(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s : %w", *obj.Key, err)
	}
	return res, nil
}

func (r *JSONParserReader) TotalRowCount(ctx context.Context) (uint64, error) {
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

func (r *JSONParserReader) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
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
		for i, line := range lines {
			cis := r.parser.Do(parsers.Message{
				Offset:     uint64(i),
				SeqNo:      0,
				Key:        []byte(filePath),
				CreateTime: s3Reader.LastModified(),
				WriteTime:  s3Reader.LastModified(),
				Value:      []byte(line),
				Headers:    nil,
			}, abstract.NewPartition(filePath, 0))
			for i := range cis {
				if parsers.IsUnparsed(cis[i]) {
					if r.unparsedPolicy == s3.UnparsedPolicyFail {
						return abstract.NewFatalError(xerrors.Errorf("unable to parse line: %s: %w", line, err))
					}
					buff = append(buff, cis[i])
					continue
				}
				cis[i].Table = r.table.Name
				cis[i].Schema = r.table.Namespace
				cis[i].PartID = filePath
				if !r.hideSystemCols {
					cis[i].ColumnValues[0] = filePath
					cis[i].ColumnValues[1] = lineCounter
				}
				buff = append(buff, cis[i])
			}

			currentSize += int64(len(line))
			lineCounter++

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

func (r *JSONParserReader) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	// the most complex and useful method in the world
	return chunk.Items
}

func (r *JSONParserReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
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

func (r *JSONParserReader) ObjectsFilter() ObjectsFilter { return IsNotEmpty }

func (r *JSONParserReader) resolveSchema(ctx context.Context, key string) (*abstract.TableSchema, error) {
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
		restCol := abstract.NewColSchema("_rest", schema.TypeAny, false)
		restCol.OriginalType = fmt.Sprintf("jsonl:%s", "string")
		cols = append(cols, restCol)
	}

	return abstract.NewTableSchema(cols), nil
}

func NewJSONParserReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (*JSONParserReader, error) {
	if src == nil || src.Format.JSONLSetting == nil {
		return nil, xerrors.New("uninitialized settings for jsonline reader")
	}

	jsonlSettings := src.Format.JSONLSetting

	reader := &JSONParserReader{
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
		metrics:        metrics,
		unparsedPolicy: src.UnparsedPolicy,
		parser:         nil,
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

	cfg := new(jsonparser.ParserConfigJSONCommon)
	cfg.AddRest = reader.unexpectedFieldBehavior == s3.Infer
	cfg.NullKeysAllowed = true
	cfg.Fields = reader.tableSchema.Columns()
	cfg.AddDedupeKeys = false
	p, err := jsonparser.NewParserJSON(cfg, false, lgr, metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct JSON parser: %w", err)
	}

	reader.parser = p
	return reader, nil
}
