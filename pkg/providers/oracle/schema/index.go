package schema

import (
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

type IndexType int

const (
	IndexTypePrimaryKey = IndexType(1)
	IndexTypeUnique     = IndexType(2)
)

type Index struct {
	table   *Table
	name    string
	typ     IndexType
	columns map[string]bool
}

func NewIndex(table *Table, name string, typ IndexType) *Index {
	return &Index{
		table:   table,
		name:    name,
		typ:     typ,
		columns: map[string]bool{},
	}
}

func (index *Index) OracleTable() *Table {
	return index.table
}

func (index *Index) OracleName() string {
	return index.name
}

func (index *Index) Name() string {
	return oracle_common.ConvertOracleName(index.OracleName())
}

func (index *Index) FullName() string {
	return oracle_common.CreateSQLName(index.OracleTable().OracleSchema().Name(), index.OracleTable().Name(), index.Name())
}

func (index *Index) Type() IndexType {
	return index.typ
}

func (index *Index) ColumnsCount() int {
	return len(index.columns)
}

func (index *Index) HasColumn(column *Column) bool {
	return index.columns[column.OracleName()]
}

func (index *Index) addColumn(column *Column) {
	index.columns[column.OracleName()] = true
}

func (index *Index) removeColumn(column *Column) {
	delete(index.columns, column.OracleName())
}
