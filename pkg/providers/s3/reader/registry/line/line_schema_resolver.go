package line

import (
	"context"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var _ s3_reader.S3SchemaResolver = (*LineSchemaResolver)(nil)

// LineSchemaResolver resolves the row schema (fixed output schema or inferred "row" column).
type LineSchemaResolver struct {
	bucket      string
	client      s3iface.S3API
	logger      log.Logger
	pathPrefix  string
	pathPattern string
	tableSchema *abstract.TableSchema
	// unparsedPolicy is used for schema-walk fall-through (same as other formats).
	unparsedPolicy s3_model.UnparsedPolicy
}

// NewLineSchemaResolver builds the line-format schema resolver.
func NewLineSchemaResolver(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3SchemaResolver, error) {
	_ = metrics // API symmetry with other formats; line resolver does not need stats.
	ts := abstract.NewTableSchema(src.OutputSchema)
	var tableSchema *abstract.TableSchema
	if len(ts.Columns()) != 0 {
		tableSchema = ts
		if !src.HideSystemCols {
			cols := tableSchema.Columns()
			userDefinedSchemaHasPkey := tableSchema.Columns().HasPrimaryKey()
			tableSchema = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
		}
	}
	return &LineSchemaResolver{
		bucket:         src.Bucket,
		client:         s3RawReaderBuilder.BuildClient(sess),
		logger:         lgr,
		pathPrefix:     src.PathPrefix,
		pathPattern:    src.PathPattern,
		tableSchema:    tableSchema,
		unparsedPolicy: src.UnparsedPolicy,
	}, nil
}

func (s *LineSchemaResolver) SchemaWalkListing() *s3_reader.SchemaWalkListing {
	return &s3_reader.SchemaWalkListing{
		TableSchema:      s.tableSchema,
		Policy:           s.unparsedPolicy,
		Bucket:           s.bucket,
		PathPrefix:       s.pathPrefix,
		PathPattern:      s.pathPattern,
		Client:           s.client,
		Logger:           s.logger,
		ObjectsFilter:    s3_reader.AcceptAllObjects,
		ListOpLabel:      "line.ListFiles",
		WrapInferErrorOp: "line.ResolveSchema.walk",
	}
}

func (s *LineSchemaResolver) TryInferSchemaFromObject(_ context.Context, _ string) (*abstract.TableSchema, reader_error.ReaderError) {
	return abstract.NewTableSchema([]abstract.ColSchema{abstract.NewColSchema("row", ytschema.TypeBytes, false)}), nil
}

//nolint:descriptiveerrors
func (s *LineSchemaResolver) ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	return s3_reader.ExecuteSchemaResolver(ctx, s)
}
