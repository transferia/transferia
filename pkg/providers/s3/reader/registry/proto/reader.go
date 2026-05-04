package proto

import (
	"context"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	s3_reader.RegisterReader(model.ParsingFormatPROTO, NewProtoReader, NewProtoSchemaResolver)
}

const (
	defaultBlockSize = humanize.MiByte
)

var _ s3_reader.S3Reader = (*ProtoReader)(nil)

type ProtoReader struct {
	s3RawReaderBuilder s3raw.S3RawReaderBuilder
	table              abstract.TableID
	bucket             string
	client             s3iface.S3API
	logger             log.Logger
	pathPrefix         string
	blockSize          int64
	pathPattern        string
	metrics            *stats.SourceStats
	parserBuilder      parsers.ParserBuilder
	unparsedPolicy     s3_model.UnparsedPolicy
	schemaResolver     s3_reader.S3SchemaResolver
}

//nolint:descriptiveerrors
func NewProtoReader(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3Reader, error) {
	parserBuilder, err := newProtoParserBuilder(src, metrics)
	if err != nil {
		return nil, err
	}
	schemaResolver, err := NewProtoSchemaResolver(src, lgr, sess, metrics, s3RawReaderBuilder)
	if err != nil {
		return nil, err
	}
	return newReaderImpl(src, lgr, sess, metrics, parserBuilder, s3RawReaderBuilder, schemaResolver)
}

func (r *ProtoReader) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(r.s3RawReaderBuilder).BuildReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create S3RawReader for filePath: %s, err: %w", filePath, err)
	}
	return s3RawReader, nil
}

func (r *ProtoReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := estimateRows(ctx, r, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("unable to estimateRows for object: %s, err: %w", *obj.Key, err)
	}
	return res, nil
}

func (r *ProtoReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, s3_reader.IsNotEmpty)
	if err != nil {
		return 0, xerrors.Errorf("unable to list files, err: %w", err)
	}

	res, err := estimateRows(ctx, r, files)
	if err != nil {
		return 0, xerrors.Errorf("unable to estimateRows for files, err: %w", err)
	}
	return res, nil
}

//nolint:descriptiveerrors
func (r *ProtoReader) Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	return reader_error.WrapReaderError("proto.Read.readFileAndParse", readFileAndParse(ctx, r, schema, filePath, pusher))
}
