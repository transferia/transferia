package snapshot

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract2"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
)

type OracleDataObjects struct {
	database    *oracle_schema.Database
	schemaIndex int
	talbeIndex  int
	closed      bool
	filter      abstract2.DataObjectFilter
	lastErr     error
}

func NewOracleDataObjects(database *oracle_schema.Database, filter abstract2.DataObjectFilter) *OracleDataObjects {
	return &OracleDataObjects{
		database:    database,
		schemaIndex: 0,
		talbeIndex:  -1,
		closed:      false,
		lastErr:     nil,
		filter:      filter,
	}
}

func (dataObjects *OracleDataObjects) Database() *oracle_schema.Database {
	return dataObjects.database
}

// Begin of base DataObjects interface

func (dataObjects *OracleDataObjects) Next() bool {
	for dataObjects.next() {
		o, err := dataObjects.Object()
		if err != nil {
			dataObjects.lastErr = err
			dataObjects.closed = true
			return false
		}
		if dataObjects.filter == nil {
			return true
		}
		if ok, err := dataObjects.filter.Includes(o); ok {
			return true
		} else if err != nil {
			dataObjects.lastErr = err
			dataObjects.closed = true
			return false
		}
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

	dataObjects.talbeIndex++

	if dataObjects.talbeIndex >= dataObjects.database.OracleSchema(dataObjects.schemaIndex).TablesCount() {
		dataObjects.schemaIndex++
		dataObjects.talbeIndex = 0

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

func (dataObjects *OracleDataObjects) Object() (abstract2.DataObject, error) {
	if dataObjects.closed {
		return nil, xerrors.New("Iterator is closed")
	}

	return NewOracleDataObject(
			dataObjects.database.OracleSchema(dataObjects.schemaIndex).OracleTable(dataObjects.talbeIndex)),
		nil
}

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
				//nolint:descriptiveerrors
				return nil, err
			}
			oldTable, err := table.ToOldTable()
			if err != nil {
				//nolint:descriptiveerrors
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

// End of base DataObjects interface
