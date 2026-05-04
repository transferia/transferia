package csv

import (
	"context"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ s3_reader.S3SchemaResolver = (*CSVSchemaResolver)(nil)

// CSVSchemaResolver resolves CSV table schema from configured output schema or S3 sample (stateless; ReaderContractor caches).
type CSVSchemaResolver struct {
	config      csvConfig
	tableSchema *abstract.TableSchema
}

func (s *CSVSchemaResolver) finalizeInferredSchema(schema *abstract.TableSchema) *abstract.TableSchema {
	out := schema
	if !s.config.hideSystemCols {
		cols := out.Columns()
		userDefinedSchemaHasPkey := out.Columns().HasPrimaryKey()
		out = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
	}
	return out
}

func (s *CSVSchemaResolver) SchemaWalkListing() *s3_reader.SchemaWalkListing {
	return &s3_reader.SchemaWalkListing{
		TableSchema:      s.tableSchema,
		Policy:           s.config.unparsedPolicy,
		Bucket:           s.config.bucket,
		PathPrefix:       s.config.pathPrefix,
		PathPattern:      s.config.pathPattern,
		Client:           s.config.client,
		Logger:           s.config.logger,
		ObjectsFilter:    s3_reader.AcceptAllObjects,
		ListOpLabel:      "csv.ListFiles",
		WrapInferErrorOp: "csv.ResolveSchema.resolveSchemaData",
	}
}

func (s *CSVSchemaResolver) TryInferSchemaFromObject(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError) {
	sch, err := csvInferTableSchemaFromSample(ctx, &s.config, key)
	if err != nil {
		return nil, err
	}
	return s.finalizeInferredSchema(sch), nil
}

//nolint:descriptiveerrors
func (s *CSVSchemaResolver) ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	return s3_reader.ExecuteSchemaResolver(ctx, s)
}

// NewCSVSchemaResolver builds the schema resolver for CSV.
func NewCSVSchemaResolver(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3SchemaResolver, error) {
	config, tableSchema, err := newCSVConfigFromSource(src, lgr, sess, s3RawReaderBuilder, metrics)
	if err != nil {
		return nil, err
	}
	return &CSVSchemaResolver{config: *config, tableSchema: tableSchema}, nil
}
