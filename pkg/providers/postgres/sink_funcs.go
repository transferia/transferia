package postgres

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func isToastedRow(row *abstract.ChangeItem, schema []abstract.ColSchema) bool {
	return len(row.ColumnNames) != len(schema)
}

func getGeneratedCols(schema []abstract.ColSchema) map[string]bool {
	genColumnsSet := make(map[string]bool)
	for _, cs := range schema {
		if cs.Expression != "" {
			genColumnsSet[cs.ColumnName] = true
		}
	}
	return genColumnsSet
}

func buildOnConflictClause(keys map[string][]string, table string, colNames []string) string {
	keyCols, ok := keys[table]
	if !ok || len(keyCols) == 0 || len(colNames) == 0 {
		return ""
	}

	excludedNames := make([]string, len(colNames))
	for i := range colNames {
		excludedNames[i] = "excluded." + colNames[i]
	}

	return fmt.Sprintf(
		" on conflict (%v) do update set (%v)=row(%v)",
		strings.Join(keyCols, ", "),
		strings.Join(colNames, ", "),
		strings.Join(excludedNames, ", "),
	)
}

func BuildBulkInsertQuery(
	table string,
	schema []abstract.ColSchema,
	keys map[string][]string, // table name -> key column names
	maxPostgresQueryBytes uint64,
	items []abstract.ChangeItem,
) ([]bulkInsertQuery, error) {
	if len(items) == 0 {
		return nil, nil
	}

	generatedCols := getGeneratedCols(schema)

	// Use table schema as a source of truth for column order and types.
	colNames := make([]string, 0, len(schema))
	schemaIdxs := make([]int, 0, len(schema)) // same index as colNames
	colPlainNames := make([]string, 0, len(schema))
	for i := range schema {
		colName := schema[i].ColumnName
		if generatedCols[colName] {
			continue
		}
		colPlainNames = append(colPlainNames, colName)
		colNames = append(colNames, fmt.Sprintf("\"%v\"", colName))
		schemaIdxs = append(schemaIdxs, i)
	}
	if len(colNames) == 0 {
		return nil, xerrors.Errorf("no columns to insert for %s after filtering generated columns", table)
	}

	castSuffixes := make([]string, len(schemaIdxs))
	for i, schemaIdx := range schemaIdxs {
		colSchema := schema[schemaIdx]
		if strings.HasPrefix(colSchema.OriginalType, "pg:") {
			if IsUserDefinedType(&colSchema) {
				dataType, err := deriveUserDefinedPgDataType(&colSchema)
				if err != nil {
					return nil, xerrors.Errorf("failed to derive user defined type for column %q: %w", colSchema.ColumnName, err)
				}
				castSuffixes[i] = "::" + dataType
			} else {
				castSuffixes[i] = "::" + strings.TrimPrefix(colSchema.OriginalType, "pg:")
			}
		}
	}

	// Build required value indices once from the first row and validate row layout on subsequent rows.
	firstRow := items[0]
	firstRowColIdx := firstRow.ColumnNameIndices()
	valueIdxs := make([]int, len(colPlainNames))
	for i, colName := range colPlainNames {
		valIdx, ok := firstRowColIdx[colName]
		if !ok {
			return nil, xerrors.Errorf("multi-row insert requires column %q to be present in change item for table %s", colName, table)
		}
		valueIdxs[i] = valIdx
	}

	header := fmt.Sprintf("insert into %v (%v) values ", table, strings.Join(colNames, ", "))
	trailer := buildOnConflictClause(keys, table, colNames) + ";"

	// Limit statement size similarly to the old path.
	headerBytes := uint64(len(header))
	trailerBytes := uint64(len(trailer))
	if headerBytes+trailerBytes >= maxPostgresQueryBytes {
		return nil, xerrors.Errorf("statement overhead too large for %s", table)
	}

	var (
		stmts        []bulkInsertQuery
		currentRows  int
		baseCap      = len(header) + len(trailer)
		statementCap = baseCap
	)
	var sb bytes.Buffer

	maxGrowCap := int(maxPostgresQueryBytes)
	if maxGrowCap <= 0 {
		maxGrowCap = baseCap
	}

	reset := func() {
		sb.Reset()
		// Adaptive pre-grow: reuse last observed statement size with a small headroom.
		// This keeps growSlice pressure lower without reserving maxBytes for every statement.
		// growCap := min(statementCap+statementCap/8, maxGrowCap)
		sb.Grow(maxGrowCap)
		sb.WriteString(header)
		currentRows = 0
	}
	flush := func() {
		if currentRows == 0 {
			return
		}
		sb.WriteString(trailer)
		q := sb.String()
		if len(q) > statementCap {
			statementCap = len(q)
		}
		stmts = append(stmts, bulkInsertQuery{
			query: q,
			rows:  currentRows,
		})
	}

	writeTuple := func(w *bytes.Buffer, row abstract.ChangeItem) error {
		_ = w.WriteByte('(')
		for j, schemaIdx := range schemaIdxs {
			valIdx := valueIdxs[j]
			if valIdx >= len(row.ColumnValues) || valIdx >= len(row.ColumnNames) || row.ColumnNames[valIdx] != colPlainNames[j] {
				return xerrors.Errorf(
					"incompatible change item column layout for table %s: expected column %q at position %d",
					table,
					colPlainNames[j],
					valIdx,
				)
			}
			if j > 0 {
				_, _ = w.WriteString(", ")
			}
			if err := recursiveRepresentToWriter(w, row.ColumnValues[valIdx], schema[schemaIdx]); err != nil {
				return xerrors.Errorf("failed to represent value for column %q: %w", colPlainNames[j], err)
			}
			_, _ = w.WriteString(castSuffixes[j])
		}
		_ = w.WriteByte(')')
		return nil
	}

	reset()
	var tupleBuf bytes.Buffer
	for _, row := range items {
		tupleBuf.Reset()
		if err := writeTuple(&tupleBuf, row); err != nil {
			return nil, err
		}
		tupleBytes := uint64(tupleBuf.Len())

		// Preflight resulting size if this tuple is appended.
		extraSep := uint64(0)
		if currentRows > 0 {
			extraSep = 2 // ", "
		}
		estimatedBytes := uint64(sb.Len()) + extraSep + tupleBytes + trailerBytes
		if estimatedBytes > maxPostgresQueryBytes {
			// If this tuple alone doesn't fit, refuse fast path for this table.
			if currentRows == 0 {
				return nil, xerrors.Errorf("single row tuple too large for multi-row insert into %s", table)
			}
			flush()
			reset()
		}

		if currentRows > 0 {
			sb.WriteString(", ")
		}
		_, _ = sb.Write(tupleBuf.Bytes())
		currentRows++
	}
	flush()

	return stmts, nil
}
