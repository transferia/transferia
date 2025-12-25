package proto

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferia/transferia/pkg/providers/s3"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	abstract_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	abstract_reader.RegisterReader(model.ParsingFormatPROTO, NewProtoReader)
}

const (
	defaultBlockSize = humanize.MiByte
)

var (
	_ abstract_reader.Reader             = (*ProtoReader)(nil)
	_ abstract_reader.RowsCountEstimator = (*ProtoReader)(nil)
)

type ProtoReader struct {
	table          abstract.TableID
	bucket         string
	client         s3iface.S3API
	downloader     *s3manager.Downloader
	logger         log.Logger
	tableSchema    *abstract.TableSchema
	pathPrefix     string
	blockSize      int64
	pathPattern    string
	metrics        *stats.SourceStats
	parserBuilder  parsers.ParserBuilder
	unparsedPolicy s3.UnparsedPolicy
}

func NewProtoReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (abstract_reader.Reader, error) {
	if len(src.Format.ProtoParser.DescFile) == 0 {
		return nil, xerrors.New("desc file required")
	}
	// this is magic field to get descriptor from YT
	if len(src.Format.ProtoParser.DescResourceName) != 0 {
		return nil, xerrors.New("desc resource name is not supported by S3 source")
	}
	cfg := new(protoparser.ProtoParserConfig)
	cfg.IncludeColumns = src.Format.ProtoParser.IncludeColumns
	cfg.PrimaryKeys = src.Format.ProtoParser.PrimaryKeys
	cfg.NullKeysAllowed = src.Format.ProtoParser.NullKeysAllowed
	if err := cfg.SetDescriptors(
		src.Format.ProtoParser.DescFile,
		src.Format.ProtoParser.MessageName,
		src.Format.ProtoParser.PackageType,
	); err != nil {
		return nil, xerrors.Errorf("SetDescriptors error: %v", err)
	}
	cfg.SetLineSplitter(src.Format.ProtoParser.PackageType)
	cfg.SetScannerType(src.Format.ProtoParser.PackageType)

	parserBuilder, err := protoparser.NewLazyProtoParserBuilder(cfg, metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct proto parser: %w", err)
	}
	return newReaderImpl(src, lgr, sess, metrics, parserBuilder)
}

func (r *ProtoReader) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	sr, err := s3raw.NewS3RawReader(ctx, r.client, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader at: %w", err)
	}
	return sr, nil
}

func (r *ProtoReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := estimateRows(ctx, r, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s : %w", *obj.Key, err)
	}
	return res, nil
}

func (r *ProtoReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, r.ObjectsFilter())
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list: %w", err)
	}

	res, err := estimateRows(ctx, r, files)
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate total rows: %w", err)
	}
	return res, nil
}

func (r *ProtoReader) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
	return readFileAndParse(ctx, r, filePath, pusher)
}

func (r *ProtoReader) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	// the most complex and useful method in the world
	return chunk.Items
}

func (r *ProtoReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	if r.tableSchema != nil && len(r.tableSchema.Columns()) != 0 {
		return r.tableSchema, nil
	}

	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, aws.Int(1), r.ObjectsFilter())
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	if len(files) < 1 {
		return nil, xerrors.Errorf("unable to resolve schema, no files found: %s", r.pathPrefix)
	}

	return resolveSchema(ctx, r, *files[0].Key)
}

func (r *ProtoReader) ObjectsFilter() abstract_reader.ObjectsFilter {
	return abstract_reader.IsNotEmpty
}

func newReaderImpl(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats, parserBuilder parsers.ParserBuilder) (*ProtoReader, error) {
	reader := &ProtoReader{
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:         src.Bucket,
		client:         aws_s3.New(sess),
		downloader:     s3manager.NewDownloader(sess),
		logger:         lgr,
		tableSchema:    abstract.NewTableSchema(src.OutputSchema),
		pathPrefix:     src.PathPrefix,
		blockSize:      defaultBlockSize,
		pathPattern:    src.PathPattern,
		metrics:        metrics,
		parserBuilder:  parserBuilder,
		unparsedPolicy: src.UnparsedPolicy,
	}

	if len(reader.tableSchema.Columns()) == 0 {
		var err error
		reader.tableSchema, err = reader.ResolveSchema(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve schema: %w", err)
		}
	}

	return reader, nil
}
