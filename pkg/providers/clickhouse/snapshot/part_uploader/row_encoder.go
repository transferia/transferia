package part_uploader

import (
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
)

// rowEncoder owns one reusable argument slice. Append copies its
// values into the driver's column buffers before the next row reuses the slice.
type rowEncoder struct {
	sourceSchema            *abstract.TableSchema
	columnNames             []string
	insertColumnIndexByName map[string]int
	conversionColumns       []abstract.ColSchema
	stringPassthrough       []bool
	targetColumnTypes       []column.Type
	argumentBuffer          []any
	seenColumns             []bool
	includeTransferColumns  bool
}

func newRowEncoder(item abstract.ChangeItem, targetColumns []column.Interface, includeTransferColumns bool) (*rowEncoder, error) {
	if item.TableSchema == nil {
		return nil, abstract.NewFatalError(xerrors.New("snapshot INSERT has no table schema"))
	}
	argumentCount := len(item.ColumnNames)
	if includeTransferColumns {
		argumentCount += 2
	}
	if len(targetColumns) != argumentCount {
		return nil, abstract.NewFatalError(xerrors.Errorf("snapshot INSERT has %d columns, ClickHouse expects %d", argumentCount, len(targetColumns)))
	}
	sourceColumnByName := make(map[string]abstract.ColSchema, len(item.TableSchema.Columns()))
	for _, sourceColumn := range item.TableSchema.Columns() {
		sourceColumnByName[sourceColumn.ColumnName] = sourceColumn
	}
	encoder := &rowEncoder{
		sourceSchema:            item.TableSchema,
		columnNames:             slices.Clone(item.ColumnNames),
		insertColumnIndexByName: make(map[string]int, len(item.ColumnNames)),
		conversionColumns:       make([]abstract.ColSchema, len(item.ColumnNames)),
		stringPassthrough:       make([]bool, len(item.ColumnNames)),
		targetColumnTypes:       make([]column.Type, len(targetColumns)),
		argumentBuffer:          make([]any, argumentCount),
		seenColumns:             make([]bool, len(item.ColumnNames)),
		includeTransferColumns:  includeTransferColumns,
	}
	for columnIndex, targetColumn := range targetColumns {
		encoder.targetColumnTypes[columnIndex] = targetColumn.Type()
	}
	for columnIndex, columnName := range item.ColumnNames {
		conversionColumn, found := sourceColumnByName[columnName]
		if !found {
			return nil, abstract.NewFatalError(xerrors.Errorf("snapshot column %q is absent from the source schema", columnName))
		}
		if _, duplicate := encoder.insertColumnIndexByName[columnName]; duplicate {
			return nil, abstract.NewFatalError(xerrors.Errorf("duplicate snapshot column %q", columnName))
		}
		if targetColumns[columnIndex].Name() != columnName {
			return nil, abstract.NewFatalError(xerrors.Errorf("snapshot column %q does not match ClickHouse column %q", columnName, targetColumns[columnIndex].Name()))
		}
		// Match buildSchemaFromChangeItem: native INSERT requires the target's
		// integer width, even when the source uses a wider integer type.
		targetTypeDescription := columntypes.NewTypeDescription(string(targetColumns[columnIndex].Type()))
		if targetTypeDescription.IsInteger {
			conversionColumn.DataType = targetTypeDescription.YtBaseType
		}
		encoder.conversionColumns[columnIndex] = conversionColumn
		encoder.insertColumnIndexByName[columnName] = columnIndex
		if conversionColumn.DataType == "string" || conversionColumn.DataType == "utf8" {
			// These source types interpret string values as base64. Keep their
			// conversion rules in Restore rather than bypassing them here.
			requiresBase64Decoding := abstract.IsMysqlBinaryType(conversionColumn.OriginalType) ||
				conversionColumn.OriginalType == "pg:bytea" ||
				strings.HasPrefix(conversionColumn.OriginalType, "mysql:binary") ||
				strings.HasPrefix(conversionColumn.OriginalType, "mysql:blob") ||
				conversionColumn.OriginalType == "ydb:String"
			targetTypeName := targetColumns[columnIndex].Type()
			encoder.stringPassthrough[columnIndex] = !requiresBase64Decoding && (targetTypeName == "String" || targetTypeName == "Nullable(String)")
		}
	}
	return encoder, nil
}

func (e *rowEncoder) prepareRowArguments(item *abstract.ChangeItem) ([]any, error) {
	if item.Kind != abstract.InsertKind {
		return nil, abstract.NewFatalError(xerrors.Errorf("snapshot native writer received %s instead of INSERT", item.Kind))
	}
	if len(item.ColumnNames) != len(e.conversionColumns) || len(item.ColumnValues) != len(e.conversionColumns) {
		return nil, abstract.NewFatalError(xerrors.New("snapshot INSERT column names and values have inconsistent lengths"))
	}
	if item.TableSchema == nil {
		return nil, abstract.NewFatalError(xerrors.New("snapshot INSERT has no table schema"))
	}
	if item.TableSchema != e.sourceSchema {
		if err := validateSourceSchemaColumnNames(e.sourceSchema.Columns(), item.TableSchema.Columns()); err != nil {
			return nil, xerrors.Errorf("snapshot source schema changed: %w", err)
		}
	}
	if slices.Equal(item.ColumnNames, e.columnNames) {
		for insertColumnIndex := range e.conversionColumns {
			e.argumentBuffer[insertColumnIndex] = e.restoreColumnValue(insertColumnIndex, item.ColumnValues[insertColumnIndex])
		}
	} else {
		// Different column order is unch_eo_common. Reorder directly into the scratch
		// slice instead of copying ChangeItems and allocating one slice per row.
		clear(e.seenColumns)
		for inputColumnIndex, columnName := range item.ColumnNames {
			insertColumnIndex, found := e.insertColumnIndexByName[columnName]
			if !found || e.seenColumns[insertColumnIndex] {
				return nil, abstract.NewFatalError(xerrors.Errorf("unexpected or duplicate snapshot column %q", columnName))
			}
			e.seenColumns[insertColumnIndex] = true
			e.argumentBuffer[insertColumnIndex] = e.restoreColumnValue(insertColumnIndex, item.ColumnValues[inputColumnIndex])
		}
	}
	if e.includeTransferColumns {
		e.argumentBuffer[len(e.conversionColumns)] = item.CommitTime
		e.argumentBuffer[len(e.conversionColumns)+1] = uint64(0)
	}
	return e.argumentBuffer, nil
}

func (e *rowEncoder) restoreColumnValue(columnIndex int, value any) any {
	if e.stringPassthrough[columnIndex] {
		switch value.(type) {
		case string, []byte:
			// String.AppendRow copies bytes directly into its own column buffer.
			// Returning the original interface avoids an intermediate string copy
			// and allocation. LowCardinality is excluded: it requires map keys.
			return value
		}
	}
	return columntypes.Restore(e.conversionColumns[columnIndex], value)
}

// A different stage table can reuse the conversion plan when it has the same
// columns. Compare the server sample before reusing it across table names.
func (e *rowEncoder) canReuseForTargetColumns(targetColumns []column.Interface) bool {
	if len(targetColumns) != len(e.targetColumnTypes) {
		return false
	}
	for columnIndex, targetColumn := range targetColumns {
		if targetColumn.Type() != e.targetColumnTypes[columnIndex] {
			return false
		}
		if columnIndex < len(e.columnNames) && targetColumn.Name() != e.columnNames[columnIndex] {
			return false
		}
	}
	return true
}
