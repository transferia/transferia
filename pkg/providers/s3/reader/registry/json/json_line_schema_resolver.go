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

var _ s3_reader.S3SchemaResolver = (*JSONLineSchemaResolver)(nil)

// JSONLineSchemaResolver resolves JSONL schema from sample or configured output schema.
type JSONLineSchemaResolver struct {
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

func (s *JSONLineSchemaResolver) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(s.s3RawReaderBuilder).BuildReader(ctx, s.client, s.bucket, filePath, s.metrics)
	if err != nil {
		return nil, xerrors.Errorf("failed to new S3RawReader for key: %s, err: %w", filePath, err)
	}
	return s3RawReader, nil
}

func (s *JSONLineSchemaResolver) applyAfterInfer(schema *abstract.TableSchema) *abstract.TableSchema {
	out := schema
	if !s.hideSystemCols {
		cols := out.Columns()
		userDefinedSchemaHasPkey := out.Columns().HasPrimaryKey()
		out = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
	}
	return out
}

//nolint:descriptiveerrors
func (s *JSONLineSchemaResolver) resolveSchema(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError) {
	buff, rerr := s3_reader.ReadSchemaInferenceFirstChunk(
		ctx,
		key,
		s.pathPrefix,
		int(s.blockSize),
		s.logger,
		func() (s3raw.S3RawReader, error) { return s.newS3RawReader(ctx, key) },
		"jsonl.resolveSchema.newS3RawReader",
		"chunk.ReadNextChunk",
		"jsonl.resolveSchema.empty_sample",
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
		line, err = readSingleJSONObject(reader, key)
		if err != nil {
			return nil, reader_error.NewReaderErrorDataSchema("jsonl.resolveSchema.read_multiline_sample", key, err)
		}
	} else {
		line, err = reader.ReadString('\n')
		if err != nil && !xerrors.Is(err, io.EOF) {
			return nil, reader_error.NewReaderErrorDataSchema("jsonl.resolveSchema.read_line_sample", key, err)
		}
	}

	if err := fastjson.Validate(line); err != nil {
		return nil, reader_error.NewReaderErrorDataSchema(
			"jsonl.resolveSchema.validate",
			key,
			xerrors.Errorf("failed to validate json line: %w", err),
		)
	}

	unmarshaledJSONLine := make(map[string]any)
	if err := json.Unmarshal([]byte(line), &unmarshaledJSONLine); err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("jsonl.resolveSchema.unmarshal", key, err)
	}

	keys := util.MapKeysInOrder(unmarshaledJSONLine)
	var cols []abstract.ColSchema

	for _, colKey := range keys {
		val := unmarshaledJSONLine[colKey]
		if val == nil {
			col := abstract.NewColSchema(colKey, ytschema.TypeAny, false)
			col.OriginalType = fmt.Sprintf("jsonl:%s", "null")
			cols = append(cols, col)
			continue
		}

		valueType, originalType, err := guessType(val)
		if err != nil {
			return nil, reader_error.NewReaderErrorDataSchema("jsonl.resolveSchema.guessType", key, err)
		}

		col := abstract.NewColSchema(colKey, valueType, false)
		col.OriginalType = fmt.Sprintf("jsonl:%s", originalType)
		cols = append(cols, col)
	}

	if s.unexpectedFieldBehavior == s3_model.Infer {
		restCol := abstract.NewColSchema(RestColumnName, ytschema.TypeAny, false)
		restCol.OriginalType = fmt.Sprintf("jsonl:%s", "string")
		cols = append(cols, restCol)
	}

	return abstract.NewTableSchema(cols), nil
}

func (s *JSONLineSchemaResolver) SchemaWalkListing() *s3_reader.SchemaWalkListing {
	return &s3_reader.SchemaWalkListing{
		TableSchema:      s.tableSchema,
		Policy:           s.unparsedPolicy,
		Bucket:           s.bucket,
		PathPrefix:       s.pathPrefix,
		PathPattern:      s.pathPattern,
		Client:           s.client,
		Logger:           s.logger,
		ObjectsFilter:    s3_reader.AcceptAllObjects,
		ListOpLabel:      "jsonl.ListFiles",
		WrapInferErrorOp: "jsonl.ResolveSchema.resolveSchema",
	}
}

func (s *JSONLineSchemaResolver) TryInferSchemaFromObject(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError) {
	sch, err := s.resolveSchema(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.applyAfterInfer(sch), nil
}

//nolint:descriptiveerrors
func (s *JSONLineSchemaResolver) ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	return s3_reader.ExecuteSchemaResolver(ctx, s)
}

// NewJSONLineSchemaResolver builds the JSONL schema resolver.
func NewJSONLineSchemaResolver(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3SchemaResolver, error) {
	if src == nil || src.Format.JSONLSetting == nil {
		return nil, xerrors.New("jsonl.reader must set format.JSONLSetting")
	}
	jsonlSettings := src.Format.JSONLSetting
	ts := abstract.NewTableSchema(src.OutputSchema)
	if len(ts.Columns()) != 0 && !src.HideSystemCols {
		cols := ts.Columns()
		userDefinedSchemaHasPkey := ts.Columns().HasPrimaryKey()
		ts = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
	}
	return &JSONLineSchemaResolver{
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
