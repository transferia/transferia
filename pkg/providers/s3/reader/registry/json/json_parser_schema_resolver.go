package json

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/goccy/go-json"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/valyala/fastjson"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var _ s3_reader.S3SchemaResolver = (*JSONParserSchemaResolver)(nil)

// JSONParserSchemaResolver resolves JSON-per-line schema from config or S3 sample.
type JSONParserSchemaResolver struct {
	s3RawReaderBuilder      s3raw.S3RawReaderBuilder
	bucket                  string
	client                  s3iface.S3API
	logger                  log.Logger
	hideSystemCols          bool
	pathPrefix              string
	newlinesInValue         bool
	unexpectedFieldBehavior s3_model.UnexpectedFieldBehavior
	blockSize               int64
	pathPattern             string
	metrics                 *stats.SourceStats
	tableSchema             *abstract.TableSchema
	unparsedPolicy          s3_model.UnparsedPolicy
}

func (s *JSONParserSchemaResolver) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	return s3raw.CoalesceS3RawReaderBuilder(s.s3RawReaderBuilder).BuildReader(ctx, s.client, s.bucket, filePath, s.metrics)
}

//nolint:descriptiveerrors
func (s *JSONParserSchemaResolver) resolveSchema(ctx context.Context, fileKey string) (*abstract.TableSchema, reader_error.ReaderError) {
	buff, rerr := s3_reader.ReadSchemaInferenceFirstChunk(
		ctx,
		fileKey,
		s.pathPrefix,
		int(s.blockSize),
		s.logger,
		func() (s3raw.S3RawReader, error) { return s.newS3RawReader(ctx, fileKey) },
		"json.parser.resolveSchema.newS3RawReader",
		"chunk.ReadNextChunk",
		"json.parser.resolveSchema.empty_sample",
	)
	if rerr != nil {
		return nil, rerr
	}
	if len(buff) == 0 {
		return abstract.NewTableSchema(nil), nil
	}

	reader := bufio.NewReader(bytes.NewReader(buff))
	var line string
	var err error
	if s.newlinesInValue {
		line, err = readSingleJSONObject(reader, fileKey)
		if err != nil {
			return nil, reader_error.NewReaderErrorDataSchema("json.parser.resolveSchema.read_multiline_sample", fileKey, err)
		}
	} else {
		line, err = reader.ReadString('\n')
		if err != nil && !xerrors.Is(err, io.EOF) {
			return nil, reader_error.NewReaderErrorDataSchema("json.parser.resolveSchema.read_line_sample", fileKey, err)
		}
	}

	if err := fastjson.Validate(line); err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("json.parser.resolveSchema.validate", fileKey, err)
	}

	unmarshaledJSONLine := make(map[string]any)
	if err := json.Unmarshal([]byte(line), &unmarshaledJSONLine); err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("json.parser.resolveSchema.unmarshal", fileKey, err)
	}

	keys := util.MapKeysInOrder(unmarshaledJSONLine)
	var cols []abstract.ColSchema

	for _, fieldName := range keys {
		val := unmarshaledJSONLine[fieldName]
		if val == nil {
			col := abstract.NewColSchema(fieldName, ytschema.TypeAny, false)
			col.OriginalType = fmt.Sprintf("jsonl:%s", "null")
			cols = append(cols, col)
			continue
		}

		valueType, originalType, err := guessType(val)
		if err != nil {
			return nil, reader_error.NewReaderErrorDataSchema("json.parser.resolveSchema.guessType", fileKey, err)
		}

		col := abstract.NewColSchema(fieldName, valueType, false)
		col.OriginalType = fmt.Sprintf("jsonl:%s", originalType)
		cols = append(cols, col)
	}

	if s.unexpectedFieldBehavior == s3_model.Infer {
		restCol := abstract.NewColSchema("_rest", ytschema.TypeAny, false)
		restCol.OriginalType = fmt.Sprintf("jsonl:%s", "string")
		cols = append(cols, restCol)
	}

	return abstract.NewTableSchema(cols), nil
}

func (s *JSONParserSchemaResolver) SchemaWalkListing() *s3_reader.SchemaWalkListing {
	return &s3_reader.SchemaWalkListing{
		TableSchema:      s.tableSchema,
		Policy:           s.unparsedPolicy,
		Bucket:           s.bucket,
		PathPrefix:       s.pathPrefix,
		PathPattern:      s.pathPattern,
		Client:           s.client,
		Logger:           s.logger,
		ObjectsFilter:    s3_reader.AcceptAllObjects,
		ListOpLabel:      "json.parser.ListFiles",
		WrapInferErrorOp: "json.parser.ResolveSchema.resolveSchema",
	}
}

func (s *JSONParserSchemaResolver) TryInferSchemaFromObject(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError) {
	return s.resolveSchema(ctx, key)
}

//nolint:descriptiveerrors
func (s *JSONParserSchemaResolver) ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	return s3_reader.ExecuteSchemaResolver(ctx, s)
}

// NewJSONParserSchemaResolver builds the JSON parser schema resolver.
func NewJSONParserSchemaResolver(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3SchemaResolver, error) {
	if src == nil || src.Format.JSONLSetting == nil {
		return nil, xerrors.New("json.parser.reader is nil")
	}
	jsonlSettings := src.Format.JSONLSetting
	ts := abstract.NewTableSchema(src.OutputSchema)
	return &JSONParserSchemaResolver{
		s3RawReaderBuilder:      s3RawReaderBuilder,
		bucket:                  src.Bucket,
		client:                  s3RawReaderBuilder.BuildClient(sess),
		logger:                  lgr,
		hideSystemCols:          src.HideSystemCols,
		pathPrefix:              src.PathPrefix,
		newlinesInValue:         jsonlSettings.NewlinesInValue,
		unexpectedFieldBehavior: jsonlSettings.UnexpectedFieldBehavior,
		blockSize:               jsonlSettings.BlockSize,
		pathPattern:             src.PathPattern,
		metrics:                 metrics,
		tableSchema:             ts,
		unparsedPolicy:          src.UnparsedPolicy,
	}, nil
}
