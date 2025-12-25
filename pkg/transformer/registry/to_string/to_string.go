package tostring

import (
	"encoding/json"
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

const Type = abstract.TransformerType("convert_to_string")

func init() {
	transformer.Register[Config](Type, func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		clms, err := filter.NewFilter(cfg.Columns.IncludeColumns, cfg.Columns.ExcludeColumns)
		if err != nil {
			return nil, xerrors.Errorf("unable to create columns filter: %w", err)
		}
		tbls, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
		if err != nil {
			return nil, xerrors.Errorf("unable to create tables filter: %w", err)
		}
		return &ToStringTransformer{
			Columns:        clms,
			Tables:         tbls,
			ConvertToBytes: cfg.ConvertToBytes,
			Logger:         lgr,
		}, nil
	})
}

type Config struct {
	Columns        filter.Columns `json:"columns"`
	Tables         filter.Tables  `json:"tables"`
	ConvertToBytes bool           `json:"convert_to_bytes"`
}

type ToStringTransformer struct {
	Columns        filter.Filter
	Tables         filter.Filter
	ConvertToBytes bool
	Logger         log.Logger
}

func (f *ToStringTransformer) Type() abstract.TransformerType {
	return Type
}

func (f *ToStringTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0, len(input))
	for _, item := range input {
		oldTypes := make(map[string]string)
		newTableSchema := make([]abstract.ColSchema, len(item.TableSchema.Columns()))

		for i := range item.TableSchema.Columns() {
			newTableSchema[i] = item.TableSchema.Columns()[i]
			if f.Columns.Match(item.TableSchema.Columns()[i].ColumnName) {
				oldTypes[item.TableSchema.Columns()[i].ColumnName] = item.TableSchema.Columns()[i].DataType
				newType := schema.TypeString
				if f.ConvertToBytes {
					newType = schema.TypeBytes
				}
				newTableSchema[i].DataType = string(newType)
			}
		}

		newValues := make([]interface{}, len(item.ColumnValues))
		for i, columnName := range item.ColumnNames {
			if f.Columns.Match(columnName) {
				stringValue := SerializeToString(item.ColumnValues[i], oldTypes[columnName])
				if f.ConvertToBytes {
					newValues[i] = []byte(stringValue)
				} else {
					newValues[i] = stringValue
				}
			} else {
				newValues[i] = item.ColumnValues[i]
			}
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

func (f *ToStringTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	if !filter.MatchAnyTableNameVariant(f.Tables, table) {
		return false
	}
	if f.Columns.Empty() {
		return true
	}
	for _, colSchema := range schema.Columns() {
		if f.Columns.Match(colSchema.ColumnName) {
			return true
		}
	}
	return false
}

func (f *ToStringTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	result := original.Columns().Copy()
	for i, col := range result {
		if !f.Columns.Match(col.ColumnName) {
			continue
		}
		if f.ConvertToBytes {
			result[i].DataType = schema.TypeBytes.String()
		} else {
			result[i].DataType = schema.TypeString.String()
		}
	}
	return abstract.NewTableSchema(result), nil
}

func (f *ToStringTransformer) Description() string {
	if f.Columns.Empty() {
		return "Transform to string all column values"
	}
	includeStr := trimStr(strings.Join(f.Columns.IncludeRegexp, "|"), 100)
	excludeStr := trimStr(strings.Join(f.Columns.ExcludeRegexp, "|"), 100)
	return fmt.Sprintf("Transform to string column values (include: %v, exclude: %v)", includeStr, excludeStr)
}

func trimStr(value string, maxLength int) string {
	if len(value) > maxLength {
		value = value[:maxLength]
	}
	return value
}

func SerializeToString(value interface{}, valueType string) string {
	switch valueType {
	case schema.TypeBytes.String():
		out, ok := value.([]byte)
		if ok {
			return string(out)
		}
	case schema.TypeAny.String():
		out, err := json.Marshal(value)
		if err == nil {
			return string(out)
		}
	case schema.TypeDate.String():
		if valueAsTime, ok := value.(time.Time); ok {
			return valueAsTime.UTC().Format("2006-01-02")
		}
	case schema.TypeDatetime.String(), schema.TypeTimestamp.String():
		if valueAsTime, ok := value.(time.Time); ok {
			return valueAsTime.UTC().Format(time.RFC3339Nano)
		}
	}
	return fmt.Sprintf("%v", value)
}
