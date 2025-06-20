//go:build !disable_ydb_provider

package ydb

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type tableSchemaWrapper struct {
	tableSchema  *abstract.TableSchema
	colNameToIdx map[string]int
}

func (s *tableSchemaWrapper) Set(tableSchema *abstract.TableSchema) {
	newColNameToIdx := make(map[string]int)
	for i, el := range tableSchema.Columns() {
		newColNameToIdx[el.ColumnName] = i
	}
	s.tableSchema = tableSchema
	s.colNameToIdx = newColNameToIdx
}

func (s *tableSchemaWrapper) IsAllColumnNamesKnown(event *cdcEvent) bool {
	columnValues := event.Update
	if len(columnValues) == 0 {
		columnValues = event.NewImage
	}

	for k := range columnValues {
		if _, ok := s.colNameToIdx[k]; !ok {
			return false
		}
	}
	return true
}

func newTableSchemaObj() *tableSchemaWrapper {
	return &tableSchemaWrapper{
		tableSchema:  nil,
		colNameToIdx: nil,
	}
}

//---

type schemaWrapper struct {
	tableToSchema map[string]*tableSchemaWrapper
}

func (s *schemaWrapper) Get(tablePath string) *abstract.TableSchema {
	return s.tableToSchema[tablePath].tableSchema
}

func (s *schemaWrapper) Set(tablePath string, tableSchema *abstract.TableSchema) {
	newTableSchema := newTableSchemaObj()
	newTableSchema.Set(tableSchema)
	s.tableToSchema[tablePath] = newTableSchema
}

func (s *schemaWrapper) IsAllColumnNamesKnown(tablePath string, event *cdcEvent) (bool, error) {
	if tableSchema, ok := s.tableToSchema[tablePath]; ok {
		return tableSchema.IsAllColumnNamesKnown(event), nil
	}
	return false, xerrors.Errorf("unknown tablePath: %s", tablePath)
}

func newSchemaObj() *schemaWrapper {
	return &schemaWrapper{
		tableToSchema: make(map[string]*tableSchemaWrapper),
	}
}
