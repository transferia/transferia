package changeitem

import (
	"testing"

	"github.com/transferia/transferia/pkg/abstract"
	"golang.org/x/exp/slices"
)

func UnmarshalChangeItem(t *testing.T, changeItemBuf []byte) *abstract.ChangeItem {
	result, err := abstract.UnmarshalChangeItem(changeItemBuf)
	if err != nil {
		t.FailNow()
	}
	return result
}

func UnmarshalChangeItemStr(t *testing.T, in string) *abstract.ChangeItem {
	return UnmarshalChangeItem(t, []byte(in))
}

func TableMapFromItems(items []abstract.ChangeItem) abstract.TableMap {
	tables := make(abstract.TableMap)
	for _, item := range items {
		if _, ok := tables[item.TableID()]; ok {
			continue
		}
		tables[item.TableID()] = abstract.TableInfo{
			EtaRow: 1,
			IsView: false,
			Schema: item.TableSchema,
		}
	}
	return tables
}

// RemoveColumnsFromChangeItem removes ColumnNames[i] and ColumnValues[i] where ColumnNames[i] is in columnsToRemove.
func RemoveColumnsFromChangeItem(item abstract.ChangeItem, columnsToRemove []string) abstract.ChangeItem {
	res := item
	res.ColumnNames = nil
	res.ColumnValues = nil
	for i, colName := range item.ColumnNames {
		if !slices.Contains(columnsToRemove, colName) {
			res.ColumnNames = append(res.ColumnNames, colName)
			res.ColumnValues = append(res.ColumnValues, item.ColumnValues[i])
		}
	}
	return res
}
