package schema

import (
	"math"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

type Table struct {
	schema       *Schema
	name         string
	columns      []*Column
	columnByName map[string]*Column
	indexByName  map[string]*Index
	keyIndex     *Index
	oldTable     *abstract.TableSchema
}

func NewTable(schema *Schema, tableName string) *Table {
	return &Table{
		schema:       schema,
		name:         tableName,
		columns:      []*Column{},
		columnByName: map[string]*Column{},
		indexByName:  map[string]*Index{},
		keyIndex:     nil,
		oldTable:     nil,
	}
}

func (table *Table) OracleTableID() *oracle_common.TableID {
	return oracle_common.NewTableID(table.OracleSchema().OracleName(), table.OracleName())
}

func (table *Table) OracleSchema() *Schema {
	return table.schema
}

func (table *Table) OracleName() string {
	return table.name
}

func (table *Table) OracleSQLName() string {
	return oracle_common.CreateSQLName(table.OracleSchema().OracleName(), table.OracleName())
}

func (table *Table) OracleColumn(i int) *Column {
	return table.columns[i]
}

func (table *Table) OracleColumnByName(name string) *Column {
	return table.columnByName[name]
}

func (table *Table) OracleIndexByName(name string) *Index {
	return table.indexByName[name]
}

func (table *Table) OracleKeyIndex() *Index {
	return table.keyIndex
}

func (table *Table) addColumn(column *Column) error {
	_, findOracleName := table.columnByName[column.OracleName()]
	_, findName := table.columnByName[column.OracleName()]
	if findOracleName || findName {
		return xerrors.Errorf("Duplicate column '%v'", column.OracleSQLName())
	}
	table.columnByName[column.OracleName()] = column
	table.columnByName[column.Name()] = column
	table.columns = append(table.columns, column)
	return nil
}

func (table *Table) removeColumn(columnName string) {
	var column *Column
	for i := 0; i < len(table.columns); i++ {
		if columnName == table.columns[i].OracleName() || columnName == table.columns[i].Name() {
			column = table.columns[i]
			table.columns[i] = table.columns[len(table.columns)-1]
			table.columns = table.columns[:len(table.columns)-1]
			break
		}
	}
	if column == nil {
		return
	}
	delete(table.columnByName, column.OracleName())
	delete(table.columnByName, column.Name())
}

func (table *Table) addIndex(index *Index) {
	table.indexByName[index.OracleName()] = index
}

func (table *Table) removeIndex(index *Index) {
	if index == table.keyIndex {
		table.keyIndex = nil
	}
	delete(table.indexByName, index.OracleName())
}

func (table *Table) selectKeyIndex() {
	// Try primary key
	for _, index := range table.indexByName {
		if index.Type() == IndexTypePrimaryKey {
			table.keyIndex = index
			return
		}
	}

	// Try unique
	columnsCount := math.MaxInt32
	var minColumnsCountUniqueIndex *Index
	for _, index := range table.indexByName {
		if index.Type() == IndexTypeUnique {
			if index.ColumnsCount() < columnsCount {
				columnsCount = index.ColumnsCount()
				minColumnsCountUniqueIndex = index
			}
		}
	}
	table.keyIndex = minColumnsCountUniqueIndex
}

func (table *Table) Schema() string {
	return table.OracleSchema().Name()
}

func (table *Table) Name() string {
	return oracle_common.ConvertOracleName(table.OracleName())
}

func (table *Table) FullName() string {
	return oracle_common.CreateSQLName(table.OracleSchema().Name(), table.Name())
}

func (table *Table) ColumnsCount() int {
	return len(table.columns)
}

func (table *Table) ToOldTable() (*abstract.TableSchema, error) {
	if table.oldTable == nil {
		columns := []abstract.ColSchema{}
		for _, column := range table.columns {
			oldColumn, err := column.ToOldColumn()
			if err != nil {
				return nil, err
			}
			columns = append(columns, *oldColumn)
		}
		table.oldTable = abstract.NewTableSchema(columns)
	}
	return table.oldTable, nil
}
