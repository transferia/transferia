package proto

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ s3_reader.S3SchemaResolver = (*ProtoSchemaResolver)(nil)

// ProtoSchemaResolver infers schema from a sample object or returns configured output schema.
type ProtoSchemaResolver struct {
	s3RawReaderBuilder s3raw.S3RawReaderBuilder
	bucket             string
	client             s3iface.S3API
	logger             log.Logger
	blockSize          int64
	pathPrefix         string
	pathPattern        string
	metrics            *stats.SourceStats
	parserBuilder      parsers.ParserBuilder
	tableSchema        *abstract.TableSchema
	unparsedPolicy     s3_model.UnparsedPolicy
}

func (s *ProtoSchemaResolver) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(s.s3RawReaderBuilder).BuildReader(ctx, s.client, s.bucket, filePath, s.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create S3RawReader for filePath: %s, err: %w", filePath, err)
	}
	return s3RawReader, nil
}

//nolint:descriptiveerrors
func (s *ProtoSchemaResolver) resolveSchemaSample(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError) {
	s3RawReader, err := s.newS3RawReader(ctx, key)
	if err != nil {
		return nil, reader_error.NewReaderErrorTransport("proto.resolveSchema.newS3RawReader", key, err)
	}

	chunkReader := s3_reader.NewChunkReader(s3RawReader, int(s.blockSize), s.logger)
	defer chunkReader.Close()

	err = chunkReader.ReadNextChunk()
	if err != nil && !xerrors.Is(err, io.EOF) {
		return nil, reader_error.NewReaderErrorTransport("chunk.ReadNextChunk", key, err)
	}
	if len(chunkReader.Data()) == 0 {
		return nil, reader_error.NewReaderErrorDataSchema(
			"proto.resolveSchema.empty_sample",
			key,
			fmt.Errorf("could not read sample data from file: %s", key),
		)
	}

	reader := bufio.NewReader(bytes.NewReader(chunkReader.Data()))
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("proto.resolveSchema.read_sample", key, err)
	}
	parser, err := s.parserBuilder.BuildLazyParser(constructMessage(s3RawReader.LastModified(), content, []byte(key)), abstract.NewEmptyPartition())
	if err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("proto.resolveSchema.build_parser", key, err)
	}
	item := parser.Next()
	if item == nil {
		return nil, reader_error.NewReaderErrorDataSchema(
			"proto.resolveSchema.parse_sample",
			key,
			fmt.Errorf("unable to parse sample data: %s", util.Sample(string(content), 1024)),
		)
	}
	return item.TableSchema, nil
}

func (s *ProtoSchemaResolver) SchemaWalkListing() *s3_reader.SchemaWalkListing {
	return &s3_reader.SchemaWalkListing{
		TableSchema:      s.tableSchema,
		Policy:           s.unparsedPolicy,
		Bucket:           s.bucket,
		PathPrefix:       s.pathPrefix,
		PathPattern:      s.pathPattern,
		Client:           s.client,
		Logger:           s.logger,
		ObjectsFilter:    s3_reader.AcceptAllObjects,
		ListOpLabel:      "proto.ListFiles",
		WrapInferErrorOp: "proto.ResolveSchema.resolveSchemaSample",
	}
}

func (s *ProtoSchemaResolver) TryInferSchemaFromObject(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError) {
	return s.resolveSchemaSample(ctx, key)
}

//nolint:descriptiveerrors
func (s *ProtoSchemaResolver) ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	return s3_reader.ExecuteSchemaResolver(ctx, s)
}

// NewProtoSchemaResolver builds the protobuf schema resolver (same parser setup as NewProtoReader).
func NewProtoSchemaResolver(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3SchemaResolver, error) {
	parserBuilder, err := newProtoParserBuilder(src, metrics)
	if err != nil {
		return nil, err
	}

	ts := abstract.NewTableSchema(src.OutputSchema)
	return &ProtoSchemaResolver{
		s3RawReaderBuilder: s3RawReaderBuilder,
		bucket:             src.Bucket,
		client:             s3RawReaderBuilder.BuildClient(sess),
		logger:             lgr,
		blockSize:          defaultBlockSize,
		pathPrefix:         src.PathPrefix,
		pathPattern:        src.PathPattern,
		metrics:            metrics,
		parserBuilder:      parserBuilder,
		tableSchema:        ts,
		unparsedPolicy:     src.UnparsedPolicy,
	}, nil
}
