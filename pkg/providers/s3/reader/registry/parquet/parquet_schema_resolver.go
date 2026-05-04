package parquet

import (
	"bytes"
	"context"
	"fmt"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	parquet_format "github.com/parquet-go/parquet-go/format"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var _ s3_reader.S3SchemaResolver = (*ParquetSchemaResolver)(nil)

// ParquetSchemaResolver infers or returns the configured parquet table schema.
type ParquetSchemaResolver struct {
	s3RawReaderBuilder s3raw.S3RawReaderBuilder
	bucket             string
	client             s3iface.S3API
	logger             log.Logger
	hideSystemCols     bool
	pathPrefix         string
	pathPattern        string
	metrics            *stats.SourceStats
	tableSchema        *abstract.TableSchema
	unparsedPolicy     s3_model.UnparsedPolicy
}

func (s *ParquetSchemaResolver) finalizeInferredParquetSchema(schema *abstract.TableSchema) *abstract.TableSchema {
	out := schema
	if !s.hideSystemCols {
		cols := out.Columns()
		userDefinedSchemaHasPkey := out.Columns().HasPrimaryKey()
		out = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
	}
	return out
}

func (s *ParquetSchemaResolver) SchemaWalkListing() *s3_reader.SchemaWalkListing {
	return &s3_reader.SchemaWalkListing{
		TableSchema:      s.tableSchema,
		Policy:           s.unparsedPolicy,
		Bucket:           s.bucket,
		PathPrefix:       s.pathPrefix,
		PathPattern:      s.pathPattern,
		Client:           s.client,
		Logger:           s.logger,
		ObjectsFilter:    s3_reader.AcceptAllObjects,
		ListOpLabel:      "parquet.ListFiles",
		WrapInferErrorOp: "parquet.parser.ResolveSchema.resolveSchema",
	}
}

func (s *ParquetSchemaResolver) TryInferSchemaFromObject(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError) {
	sch, err := s.resolveSchema(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.finalizeInferredParquetSchema(sch), nil
}

//nolint:descriptiveerrors
func (s *ParquetSchemaResolver) ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	return s3_reader.ExecuteSchemaResolver(ctx, s)
}

// resolveSchema infers column schema from parquet footer metadata for filePath.
//
//nolint:descriptiveerrors
func (s *ParquetSchemaResolver) resolveSchema(ctx context.Context, filePath string) (*abstract.TableSchema, reader_error.ReaderError) {
	meta, err := s.newS3RawReader(ctx, filePath)
	if err != nil {
		if data, ok := reader_error.AsReaderErrorData(err); ok {
			return nil, data
		}
		return nil, reader_error.NewReaderErrorTransport("parquet.parser.resolveSchema.newS3RawReader", filePath, err)
	}
	defer meta.Close()

	var cols []abstract.ColSchema
	for _, el := range meta.Schema().Fields() {
		if el.Type() == nil {
			continue
		}
		typ := ytschema.TypeAny
		if el.Type().PhysicalType() != nil {
			switch *el.Type().PhysicalType() {
			case parquet_format.Boolean:
				typ = ytschema.TypeBoolean
			case parquet_format.Int32:
				typ = ytschema.TypeInt32
			case parquet_format.Int64:
				typ = ytschema.TypeInt64
			case parquet_format.Float:
				typ = ytschema.TypeFloat32
			case parquet_format.Double:
				typ = ytschema.TypeFloat64
			case parquet_format.Int96:
				typ = ytschema.TypeString
			case parquet_format.ByteArray, parquet_format.FixedLenByteArray:
				typ = ytschema.TypeBytes
			default:
			}
		}
		if el.Type().LogicalType() != nil {
			lt := el.Type().LogicalType()
			switch {
			case lt.Date != nil:
				typ = ytschema.TypeDate
			case lt.UTF8 != nil:
				typ = ytschema.TypeString
			case lt.Integer != nil:
				if lt.Integer.IsSigned {
					typ = ytschema.TypeInt64
				} else {
					typ = ytschema.TypeUint64
				}
			case lt.Decimal != nil:
				if lt.Decimal.Precision > 8 {
					typ = ytschema.TypeString
				} else {
					typ = ytschema.TypeFloat64
				}
			case lt.Timestamp != nil:
				typ = ytschema.TypeTimestamp
			case lt.UUID != nil:
				typ = ytschema.TypeString
			case lt.Enum != nil:
				typ = ytschema.TypeString
			}
		}
		if el.Type().ConvertedType() != nil {
			switch *el.Type().ConvertedType() {
			case deprecated.UTF8:
				typ = ytschema.TypeString
			case deprecated.Date:
				typ = ytschema.TypeDate
			case deprecated.Decimal:
				typ = ytschema.TypeFloat64
			}
		}
		col := abstract.NewColSchema(el.Name(), typ, false)
		col.OriginalType = fmt.Sprintf("parquet:%s", el.Type().String())
		cols = append(cols, col)
	}

	return abstract.NewTableSchema(cols), nil
}

func (s *ParquetSchemaResolver) newS3RawReader(ctx context.Context, filePath string) (*parquet.Reader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(s.s3RawReaderBuilder).BuildReader(ctx, s.client, s.bucket, filePath, s.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to build reader, err: %w", err)
	}

	if readerAll, ok := s3RawReader.(s3raw.ReaderAll); ok {
		data, err := readerAll.ReadAll(ctx)
		if err != nil {
			return nil, xerrors.Errorf("unable to read all, filePath: %s, err: %w", filePath, err)
		}
		result, err := openParquetFromReaderClassifiesParquetPanics(filePath, bytes.NewReader(data))
		if err != nil {
			return nil, xerrors.Errorf(
				"parquet.newS3RawReader.open: %w",
				reader_error.NewReaderErrorDataFile("parquet.newS3RawReader.open", filePath, err),
			)
		}
		return result, nil
	}

	result, err := openParquetFromReaderClassifiesParquetPanics(filePath, s3RawReader)
	if err != nil {
		return nil, xerrors.Errorf(
			"parquet.newS3RawReader.open: %w",
			reader_error.NewReaderErrorDataFile("parquet.newS3RawReader.open", filePath, err),
		)
	}
	return result, nil
}

// NewParquetSchemaResolver builds the parquet schema resolver.
func NewParquetSchemaResolver(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	metrics *stats.SourceStats,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
) (s3_reader.S3SchemaResolver, error) {
	if src == nil {
		return nil, xerrors.New("parquet.NewParquet.uninitialized")
	}
	tableSchema := abstract.NewTableSchema(src.OutputSchema)
	if len(tableSchema.Columns()) != 0 && !src.HideSystemCols {
		cols := tableSchema.Columns()
		userDefinedSchemaHasPkey := tableSchema.Columns().HasPrimaryKey()
		tableSchema = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
	}

	return &ParquetSchemaResolver{
		s3RawReaderBuilder: s3RawReaderBuilder,
		bucket:             src.Bucket,
		client:             s3RawReaderBuilder.BuildClient(sess),
		logger:             lgr,
		hideSystemCols:     src.HideSystemCols,
		pathPrefix:         src.PathPrefix,
		pathPattern:        src.PathPattern,
		metrics:            metrics,
		tableSchema:        tableSchema,
		unparsedPolicy:     src.UnparsedPolicy,
	}, nil
}
