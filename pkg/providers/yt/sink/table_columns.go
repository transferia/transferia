// Used only in sorted_table
package sink

import (
	"github.com/transferia/transferia/pkg/abstract"
)

type columnName = string
type columnIndex = int

type tableColumns struct {
	columns []abstract.ColSchema
	byName  map[columnName]columnIndex
}

func (t *tableColumns) getByName(name columnName) (abstract.ColSchema, bool) {
	var defaultVal abstract.ColSchema
	index, ok := t.byName[name]
	if !ok {
		return defaultVal, false
	}
	return t.columns[index], true
}

func (t *tableColumns) hasKey(name columnName) bool {
	columnPos, ok := t.byName[name]
	if !ok {
		return false
	}
	return t.columns[columnPos].PrimaryKey
}

func (t *tableColumns) hasOnlyPKey() bool {
	for _, column := range t.columns {
		if !column.PrimaryKey {
			return false
		}
	}
	return true
}

func newTableColumns(columns []abstract.ColSchema) tableColumns {
	byName := make(map[columnName]columnIndex)

	for index, col := range columns {
		byName[col.ColumnName] = index
	}

	return tableColumns{
		columns: columns,
		byName:  byName,
	}
}
