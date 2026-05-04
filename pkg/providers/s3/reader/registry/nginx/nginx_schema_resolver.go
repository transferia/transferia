package reader

import (
	"context"
	"fmt"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ s3_reader.S3SchemaResolver = (*NginxSchemaResolver)(nil)

// NginxSchemaResolver holds the nginx log table schema from config (tableSchema-only listing).
type NginxSchemaResolver struct {
	tableSchema *abstract.TableSchema
}

func (s *NginxSchemaResolver) SchemaWalkListing() *s3_reader.SchemaWalkListing {
	return &s3_reader.SchemaWalkListing{
		TableSchema:      s.tableSchema,
		Policy:           "",
		Bucket:           "",
		PathPrefix:       "",
		PathPattern:      "",
		Client:           nil,
		Logger:           nil,
		ObjectsFilter:    s3_reader.AcceptAllObjects,
		ListOpLabel:      "",
		WrapInferErrorOp: "",
	}
}

func (*NginxSchemaResolver) TryInferSchemaFromObject(context.Context, string) (*abstract.TableSchema, reader_error.ReaderError) {
	return nil, reader_error.NewReaderErrorConfig(
		"nginx.schema.sample",
		xerrors.New("nginx format does not infer schema from S3 objects"),
	)
}

func (s *NginxSchemaResolver) ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	return s3_reader.ExecuteSchemaResolver(ctx, s)
}

// NewNginxSchemaResolver builds the nginx schema resolver (same schema logic as the historical combined reader constructor).
func NewNginxSchemaResolver(
	src *s3_model.S3Source,
	_ log.Logger,
	_ *aws_session.Session,
	_ *stats.SourceStats,
	_ s3raw.S3RawReaderBuilder,
) (s3_reader.S3SchemaResolver, error) {
	if src == nil || src.Format.NginxSetting == nil {
		return nil, xerrors.New("nginx.reader is nil")
	}

	compiled, err := compileFormat(src.Format.NginxSetting.Format)
	if err != nil {
		return nil, xerrors.Errorf("unable to compileFormat, err: %w", err)
	}

	tableSchema := abstract.NewTableSchema(src.OutputSchema)

	if len(tableSchema.Columns()) == 0 {
		tableSchema = abstract.NewTableSchema(compiled.schema)
	} else {
		fieldIndex := make(map[string]int, len(compiled.fields))
		for i, name := range compiled.fields {
			fieldIndex[name] = i
		}
		var cols []abstract.ColSchema
		for _, col := range tableSchema.Columns() {
			if col.Path == "" {
				idx, ok := fieldIndex[col.ColumnName]
				if !ok {
					continue
				}
				col.Path = fmt.Sprintf("%d", idx)
			}
			if col.OriginalType == "" {
				col.OriginalType = fmt.Sprintf("nginx:%s", col.DataType)
			}
			cols = append(cols, col)
		}
		tableSchema = abstract.NewTableSchema(cols)
	}

	if !src.HideSystemCols {
		cols := tableSchema.Columns()
		hasPkey := tableSchema.Columns().HasPrimaryKey()
		tableSchema = s3_reader.AppendSystemColsTableSchema(cols, !hasPkey)
	}

	return &NginxSchemaResolver{tableSchema: tableSchema}, nil
}
