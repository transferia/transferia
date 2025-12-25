package helpers

import (
	"fmt"

	"github.com/transferia/transferia/pkg/abstract"
)

type row struct {
	Name, DataType, GoType, Value string
}

type CanonTypedChangeItem []row

func ToCanonTypedChangeItem(item abstract.ChangeItem) CanonTypedChangeItem {
	res := make([]row, len(item.ColumnValues))
	for i, col := range item.TableSchema.Columns() {
		res[i] = row{
			Name:     col.ColumnName,
			DataType: col.DataType,
			GoType:   fmt.Sprintf("%T", item.ColumnValues[i]),
			Value:    fmt.Sprint(item.ColumnValues[i]),
		}
	}
	return res
}

func ToCanonTypedChangeItems(items []abstract.ChangeItem) []CanonTypedChangeItem {
	res := make([]CanonTypedChangeItem, len(items))
	for i, item := range items {
		res[i] = ToCanonTypedChangeItem(item)
	}
	return res
}
