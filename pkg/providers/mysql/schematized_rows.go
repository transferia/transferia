//go:build !disable_mysql_provider

package mysql

import (
	"fmt"

	"github.com/transferia/transferia/pkg/abstract"
)

type schematizedRowsEvent struct {
	event         *RowsEvent
	schema        *abstract.TableSchema
	colNames      []string
	colPosReorder map[int]int
	colOrigPos    map[string]int
}

func (sre *schematizedRowsEvent) OrigEvent() *RowsEvent {
	return sre.event
}

func (sre *schematizedRowsEvent) Schema() *abstract.TableSchema {
	return sre.schema
}

func (sre *schematizedRowsEvent) ColumnNames() []string {
	return sre.colNames
}

func (sre *schematizedRowsEvent) GetRowValues(rowIndex int) []interface{} {
	return sre.reorderVals(sre.event.Data.Rows[rowIndex])
}

func (sre *schematizedRowsEvent) HasOldValue(columnName string) bool {
	origColumnPos := sre.colOrigPos[columnName]
	return sre.event.IsColumnPresent1(uint64(origColumnPos))
}

func (sre *schematizedRowsEvent) HasNewValue(columnName string) bool {
	origColumnPos := sre.colOrigPos[columnName]
	return sre.event.IsColumnPresent2(uint64(origColumnPos))
}

func (sre *schematizedRowsEvent) GetColumnValue(rowIndex int, columnName string) interface{} {
	origColumnPos := sre.colOrigPos[columnName]
	return sre.event.Data.Rows[rowIndex][origColumnPos]
}

func (sre *schematizedRowsEvent) reorderVals(origVals []interface{}) []interface{} {
	res := make([]interface{}, len(origVals))
	for i, val := range origVals {
		newI := sre.colPosReorder[i]
		res[newI] = val
	}
	return res
}

func newSchematizedRowsEvent(event *RowsEvent, expressions map[string]string, requiredCols map[string]bool) *schematizedRowsEvent {
	schema := make([]abstract.ColSchema, len(event.Table.Columns))
	colNames := make([]string, len(event.Table.Columns))
	colPosReorder := make(map[int]int, len(event.Table.Columns))
	colOrigPos := make(map[string]int, len(event.Table.Columns))

	schemaColIndex := 0
	// add pkey columns first
	pkeyIndxs := make(map[int]bool, len(event.Table.PKColumns))
	for _, colIndex := range event.Table.PKColumns {
		pkeyIndxs[colIndex] = true

		col := event.Table.Columns[colIndex]

		colOrigPos[col.Name] = colIndex
		colPosReorder[colIndex] = schemaColIndex
		colNames[schemaColIndex] = col.Name
		schema[schemaColIndex] = abstract.ColSchema{
			TableSchema:  event.Table.Schema,
			TableName:    event.Table.Name,
			Path:         "",
			ColumnName:   col.Name,
			DataType:     TypeToYt(col.RawType).String(),
			PrimaryKey:   true,
			FakeKey:      false,
			Required:     requiredCols[col.Name],
			Expression:   expressions[fmt.Sprintf("%v.%v", event.Table.Name, col.Name)],
			OriginalType: fmt.Sprintf("mysql:%v", col.RawType),
			Properties:   nil,
		}

		schemaColIndex++
	}

	// set rest columns
	for origColIndex, col := range event.Table.Columns {
		if pkeyIndxs[origColIndex] {
			continue
		}

		colPosReorder[origColIndex] = schemaColIndex
		colOrigPos[col.Name] = origColIndex
		colNames[schemaColIndex] = col.Name
		schema[schemaColIndex] = abstract.ColSchema{
			TableSchema:  event.Table.Schema,
			TableName:    event.Table.Name,
			Path:         "",
			ColumnName:   col.Name,
			DataType:     TypeToYt(col.RawType).String(),
			PrimaryKey:   false,
			FakeKey:      false,
			Required:     requiredCols[col.Name],
			Expression:   expressions[fmt.Sprintf("%v.%v", event.Table.Name, col.Name)],
			OriginalType: fmt.Sprintf("mysql:%v", col.RawType),
			Properties:   nil,
		}
		schemaColIndex++
	}

	return &schematizedRowsEvent{
		event:         event,
		schema:        abstract.NewTableSchema(schema),
		colNames:      colNames,
		colPosReorder: colPosReorder,
		colOrigPos:    colOrigPos,
	}
}
