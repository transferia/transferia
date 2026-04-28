package snapshot

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
)

type OracleDataObjects struct {
	database    *oracle_schema.Database
	schemaIndex int
	tableIndex  int
	closed      bool
	lastErr     error
}

func NewOracleDataObjects(database *oracle_schema.Database) *OracleDataObjects {
	return &OracleDataObjects{
		database:    database,
		schemaIndex: 0,
		tableIndex:  -1,
		closed:      false,
		lastErr:     nil,
	}
}

func (dataObjects *OracleDataObjects) Database() *oracle_schema.Database {
	return dataObjects.database
}

func (dataObjects *OracleDataObjects) Next() bool {
	for dataObjects.next() {
		return true
	}
	return false
}

func (dataObjects *OracleDataObjects) next() bool {
	if dataObjects.closed {
		return false
	}

	if dataObjects.schemaIndex >= dataObjects.database.SchemasCount() {
		dataObjects.closed = true
		return false
	}

	dataObjects.tableIndex++

	if dataObjects.tableIndex >= dataObjects.database.OracleSchema(dataObjects.schemaIndex).TablesCount() {
		dataObjects.schemaIndex++
		dataObjects.tableIndex = 0

		if dataObjects.schemaIndex >= dataObjects.database.SchemasCount() {
			dataObjects.closed = true
			return false
		}
	}

	return true
}

func (dataObjects *OracleDataObjects) Err() error {
	return dataObjects.lastErr
}

func (dataObjects *OracleDataObjects) Close() {
	dataObjects.closed = true
}

func (dataObjects *OracleDataObjects) CurrentTable() (*oracle_schema.Table, error) {
	if dataObjects.closed {
		return nil, xerrors.New("Iterator is closed")
	}
	return dataObjects.database.OracleSchema(dataObjects.schemaIndex).OracleTable(dataObjects.tableIndex), nil
}

//nolint:descriptiveerrors
func (dataObjects *OracleDataObjects) ToOldTableMap() (abstract.TableMap, error) {
	if dataObjects.closed {
		return nil, xerrors.New("Iterator is closed")
	}

	tables := map[abstract.TableID]abstract.TableInfo{}
	for i := 0; i < dataObjects.database.SchemasCount(); i++ {
		schema := dataObjects.database.OracleSchema(i)
		for j := 0; j < schema.TablesCount(); j++ {
			table := schema.OracleTable(j)
			oldTableID, err := table.OracleTableID().ToOldTableID()
			if err != nil {
				return nil, err
			}
			oldTable, err := table.ToOldTable()
			if err != nil {
				return nil, err
			}
			tables[*oldTableID] = abstract.TableInfo{
				EtaRow: 0,
				IsView: false,
				Schema: oldTable,
			}
		}
	}

	dataObjects.closed = true

	return tables, nil
}
