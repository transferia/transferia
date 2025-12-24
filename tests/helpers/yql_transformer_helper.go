package helpers

import (
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
)

func MakeChangeItems(tablePath string) []abstract.ChangeItem {
	allowedColumnNames := map[string]bool{
		"id": true,

		"Bool_": true,

		"Int8_":  true,
		"Int16_": true,
		"Int32_": true,
		"Int64_": true,

		"Uint8_":  true,
		"Uint16_": true,
		"Uint32_": true,
		"Uint64_": true,

		"Float_":    true,
		"Decimal_":  true,
		"DyNumber_": true,

		"String_": true,
		"Utf8_":   true,
	}

	result := YDBInitChangeItem(tablePath)

	values := result.AsMap()
	result.ColumnNames = nil
	result.ColumnValues = nil
	for key, value := range values {
		if allowedColumnNames[key] {
			result.ColumnNames = append(result.ColumnNames, key)
			result.ColumnValues = append(result.ColumnValues, value)
		}
	}

	tableSchema := yslices.Filter(result.TableSchema.Columns(), func(col abstract.ColSchema) bool {
		return allowedColumnNames[col.ColumnName]
	})
	result.TableSchema = abstract.NewTableSchema(tableSchema)

	if len(result.ColumnNames) != len(result.ColumnValues) || len(result.ColumnNames) != len(result.TableSchema.Columns()) {
		panic("!")
	}

	return []abstract.ChangeItem{*result}
}
