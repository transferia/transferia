package todatetime

import (
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

const Type = abstract.TransformerType("convert_to_datetime")

var supportedTypes map[string]bool = map[string]bool{
	schema.TypeInt32.String():  true,
	schema.TypeUint32.String(): true,
}

func init() {
	transformer.Register[Config](Type, func(cfg Config, logger log.Logger, _ abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		clms, err := filter.NewFilter(cfg.Columns.IncludeColumns, cfg.Columns.ExcludeColumns)
		if err != nil {
			return nil, xerrors.Errorf("unable to create columns filter: %w", err)
		}
		tbls, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
		if err != nil {
			return nil, xerrors.Errorf("unable to create tables filter: %w", err)
		}
		return &ToDateTimeTransformer{
			Columns: clms,
			Tables:  tbls,
			Logger:  logger,
		}, nil
	})
}

type Config struct {
	Columns filter.Columns `json:"columns"`
	Tables  filter.Tables  `json:"tables"`
}

type ToDateTimeTransformer struct {
	Columns filter.Filter
	Tables  filter.Filter
	Logger  log.Logger
}

func (t *ToDateTimeTransformer) Type() abstract.TransformerType {
	return Type
}

func (t *ToDateTimeTransformer) suitableColumn(columnName string, columnType string) bool {
	return t.Columns.Match(columnName) && supportedTypes[columnType]
}

func (t *ToDateTimeTransformer) Suitable(table abstract.TableID, tableSchema *abstract.TableSchema) bool {
	if !filter.MatchAnyTableNameVariant(t.Tables, table) {
		return false
	}
	if t.Columns.Empty() {
		return false
	}
	for _, colSchema := range tableSchema.Columns() {
		if t.suitableColumn(colSchema.ColumnName, colSchema.DataType) {
			return true
		}
	}
	return false
}

func (t *ToDateTimeTransformer) Description() string {
	if t.Columns.Empty() {
		return "Transform to datetime uint32 column values"
	}

	includeStr := ctrcut(strings.Join(t.Columns.IncludeRegexp, "|"), 100)
	excludeStr := ctrcut(strings.Join(t.Columns.ExcludeRegexp, "|"), 100)
	return fmt.Sprintf("Transform to datetime uint32 column values (include: %s, exclude: %s)", includeStr, excludeStr)
}

func ctrcut(value string, maxLength int) string {
	return value[:min(maxLength, len(value))]
}

func (t *ToDateTimeTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0, len(input))
	for _, item := range input {
		oldTypes := make(map[string]string)
		newTableSchema := make([]abstract.ColSchema, len(item.TableSchema.Columns()))

		for i := range item.TableSchema.Columns() {
			column := item.TableSchema.Columns()[i]
			newTableSchema[i] = column
			if t.suitableColumn(column.ColumnName, column.DataType) {
				oldTypes[column.ColumnName] = column.DataType
				newTableSchema[i].DataType = schema.TypeDatetime.String()
			}
		}

		colNameToIdx := abstract.MakeMapColNameToIndex(item.TableSchema.Columns())
		newValues := make([]interface{}, len(item.ColumnNames))
		for i, columnName := range item.ColumnNames {
			column := item.TableSchema.Columns()[colNameToIdx[columnName]]
			if !t.suitableColumn(column.ColumnName, column.DataType) {
				newValues[i] = item.ColumnValues[i]
				continue
			}

			newValues[i] = SerializeToDateTime(item.ColumnValues[i], oldTypes[columnName])
		}
		item.ColumnValues = newValues
		item.SetTableSchema(abstract.NewTableSchema(newTableSchema))
		transformed = append(transformed, item)
	}
	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      nil,
	}
}

func (t *ToDateTimeTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	result := original.Columns().Copy()
	for i, col := range result {
		if t.suitableColumn(col.ColumnName, col.DataType) {
			result[i].DataType = schema.TypeDatetime.String()
		}
	}
	return abstract.NewTableSchema(result), nil
}

func SerializeToDateTime(value interface{}, valueType string) time.Time {
	switch valueType {
	case string(schema.TypeInt32):
		out, ok := value.(int32)
		if ok {
			return time.Unix(int64(out), 0)
		}
	case string(schema.TypeUint32):
		out, ok := value.(uint32)
		if ok {
			return time.Unix(int64(out), 0)
		}
	}
	return time.Unix(0, 0)
}
