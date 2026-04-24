package schema

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

type Schema struct {
	repository  *Database
	name        string
	tables      []*Table
	tableByName map[string]*Table
}

func NewSchema(repository *Database, schemaName string) *Schema {
	return &Schema{
		repository:  repository,
		name:        schemaName,
		tables:      []*Table{},
		tableByName: map[string]*Table{},
	}
}

func (schema *Schema) OracleDatabase() *Database {
	return schema.repository
}

func (schema *Schema) OracleName() string {
	return schema.name
}

func (schema *Schema) OracleSQLName() string {
	return oracle_common.CreateSQLName(schema.OracleName())
}

func (schema *Schema) Name() string {
	return oracle_common.ConvertOracleName(schema.OracleName())
}

func (schema *Schema) FullName() string {
	return schema.Name()
}

func (schema *Schema) TablesCount() int {
	return len(schema.tables)
}

func (schema *Schema) OracleTable(i int) *Table {
	return schema.tables[i]
}

func (schema *Schema) OracleTableByName(name string) *Table {
	return schema.tableByName[name]
}

func (schema *Schema) add(table *Table) error {
	_, findOracleName := schema.tableByName[table.OracleName()]
	_, findName := schema.tableByName[table.Name()]
	if findOracleName || findName {
		return xerrors.Errorf("Duplicate table '%v'", table.OracleSQLName())
	}
	schema.tableByName[table.OracleName()] = table
	schema.tableByName[table.Name()] = table
	schema.tables = append(schema.tables, table)
	return nil
}

func (schema *Schema) remove(tableName string) {
	var table *Table
	for i := 0; i < len(schema.tables); i++ {
		if tableName == schema.tables[i].OracleName() || tableName == schema.tables[i].Name() {
			table = schema.tables[i]
			schema.tables[i] = schema.tables[len(schema.tables)-1]
			schema.tables = schema.tables[:len(schema.tables)-1]
			break
		}
	}
	if table == nil {
		return
	}
	delete(schema.tableByName, table.OracleName())
	delete(schema.tableByName, table.Name())
}
