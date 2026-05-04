package json

import (
	"context"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	parser_json "github.com/transferia/transferia/pkg/parsers/registry/json"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/estimators"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ s3_reader.S3Reader = (*JSONParserReader)(nil)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatJSON, NewJSONParserReader, NewJSONParserSchemaResolver)
}

type JSONParserReader struct {
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

	parser parsers.Parser
}

func (r *JSONParserReader) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(r.s3RawReaderBuilder).BuildReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("failed to build s3 raw reader, err: %w", err)
	}
	return s3RawReader, nil
}

func (r *JSONParserReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	readAllImpl := func(in []byte) ([]string, uint64, error) {
		lines, bytesRead := readAllMultilineLines(in)
		return lines, bytesRead, nil
	}
	return estimators.EstimateRows(ctx, r.logger, files, int(r.blockSize), r.newS3RawReader, readAllImpl)
}

func (r *JSONParserReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows count for object: %s, err: %w", *obj.Key, err)
	}
	return res, nil
}

func (r *JSONParserReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, s3_reader.IsNotEmpty)
	if err != nil {
		return 0, xerrors.Errorf("failed to list files, err: %w", err)
	}

	res, err := r.estimateRows(ctx, files)
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows count for files, err: %w", err)
	}
	return res, nil
}

//nolint:descriptiveerrors
func (r *JSONParserReader) finishSchemaAndParser(schema *abstract.TableSchema) reader_error.ReaderError {
	effective := schema
	if !r.hideSystemCols {
		cols := effective.Columns()
		userDefinedSchemaHasPkey := effective.Columns().HasPrimaryKey()
		effective = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
	}
	cfg := new(parser_json.ParserConfigJSONCommon)
	cfg.AddRest = r.unexpectedFieldBehavior == s3_model.Infer
	cfg.NullKeysAllowed = true
	cfg.Fields = effective.Columns()
	cfg.AddDedupeKeys = false
	p, err := parser_json.NewParserJSON(cfg, false, r.logger, r.metrics)
	if err != nil {
		return reader_error.NewReaderErrorConfig("json.parser.NewParserJSON", err)
	}
	r.parser = p
	return nil
}

//nolint:descriptiveerrors
func (r *JSONParserReader) Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	if r.parser == nil {
		if schema == nil || len(schema.Columns()) == 0 {
			return nil
		}
		if err := r.finishSchemaAndParser(schema); err != nil {
			return err
		}
	}
	if r.parser == nil {
		return nil
	}

	s3RawReader, err := r.newS3RawReader(ctx, filePath)
	if err != nil {
		return reader_error.NewReaderErrorTransport("json.parser.Read.newS3RawReader", r.pathPrefix, err)
	}

	offset := uint64(0)
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
		data := chunkReader.Data()
		if len(data) == 0 && chunkReader.IsEOF() {
			break
		}
		if chunkReader.IsEOF() && len(data) != 0 {
			lastRound = true
		}
		if r.newlinesInValue {
			lines, readBytes = readAllMultilineLines(data)
		} else {
			var err2 reader_error.ReaderError = nil
			lines, readBytes, err2 = readAllLines(data, filePath)
			if err2 != nil {
				if handled, res := s3_reader.TryHandleDataErrorWithFlush(ctx, r.table, r.unparsedPolicy, filePath, lineCounter, 0, err2, pusher, wrapJSONParserReadFlushChunk); handled {
					return res
				}
				return reader_error.WrapReaderError("json.parser.Read.readAllLines", err2)
			}
		}

		// Limit readBytes to data length to prevent slice bounds out of range
		if readBytes > uint64(len(data)) {
			readBytes = uint64(len(data))
		}

		chunkReader.FillBuffer(data[readBytes:])
		offset += readBytes
		var buff []abstract.ChangeItem
		var currentSize int64
		for i, line := range lines {
			changeItems := r.parser.Do(
				parsers.Message{
					Offset:     uint64(i),
					SeqNo:      0,
					Key:        []byte(filePath),
					CreateTime: s3RawReader.LastModified(),
					WriteTime:  s3RawReader.LastModified(),
					Value:      []byte(line),
					Headers:    nil,
				}, abstract.NewEmptyPartition(),
			)
			for j := range changeItems {
				if parsers.IsUnparsed(changeItems[j]) {
					switch r.unparsedPolicy {
					case s3_model.UnparsedPolicyFail:
						return reader_error.NewReaderErrorFatal("unable to parse line", xerrors.Errorf("unable to parse line: %q", line))
					case s3_model.UnparsedPolicyRetry:
						return reader_error.NewReaderErrorDataRecord("json.parser.Read.unparsedRetry", filePath, uint64(i), xerrors.Errorf("parser returned unparsed item (retry), line:%d", lineCounter))
					default:
						buff = append(buff, changeItems[j])
					}
					continue
				}
				changeItems[j].Table = r.table.Name
				changeItems[j].Schema = r.table.Namespace
				changeItems[j].PartID = filePath
				if !r.hideSystemCols {
					changeItems[j].ColumnValues[0] = filePath
					changeItems[j].ColumnValues[1] = lineCounter
				}
				buff = append(buff, changeItems[j])
			}

			currentSize += int64(len(line))
			lineCounter++

			if len(buff) > r.batchSize {
				if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
					return wrapJSONParserReadFlushChunk(err)
				}
				currentSize = 0
				buff = make([]abstract.ChangeItem, 0)
			}
		}
		if err := s3_reader.FlushChunk(ctx, filePath, lineCounter, currentSize, buff, pusher); err != nil {
			return wrapJSONParserReadFlushChunk(err)
		}
	}

	return nil
}

func NewJSONParserReader(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3Reader, error) {
	if src == nil || src.Format.JSONLSetting == nil {
		return nil, xerrors.New("json.parser.reader is nil")
	}

	jsonlSettings := src.Format.JSONLSetting

	return &JSONParserReader{
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
		parser:         nil,
	}, nil
}
