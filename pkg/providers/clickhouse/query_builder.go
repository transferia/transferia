//go:build !disable_clickhouse_provider

package clickhouse

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
)

func buildSelectQuery(table *abstract.TableDescription, tableColumns abstract.TableColumns, isHomo, deletable bool, additionalCond string) string {
	columns := make([]string, 0)
	for _, col := range tableColumns {
		if ColumnShouldBeSelected(col, isHomo) {
			if strings.HasPrefix(col.OriginalType, "ch:Decimal(") {
				columns = append(columns, fmt.Sprintf("toString(`%s`)", col.ColumnName))
			} else {
				columns = append(columns, fmt.Sprintf("`%s`", col.ColumnName))
			}
		}
	}

	finalStmt := ""
	if deletable {
		finalStmt = " FINAL "
	}

	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` %s WHERE 1=1 ", strings.Join(columns, ","), table.Schema, table.Name, finalStmt)
	if table.Filter != "" {
		query += fmt.Sprintf(" AND (%s)", table.Filter)
	}
	if additionalCond != "" {
		query += fmt.Sprintf(" AND (%s)", additionalCond)
	}
	query += getDeleteTimeFilterExpr(deletable)
	primaryKeys := make([]string, 0)
	for _, col := range tableColumns {
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.ColumnName)
		}
	}
	if len(primaryKeys) > 0 {
		query += fmt.Sprintf(" ORDER BY %s", strings.Join(primaryKeys, ", "))
	}
	if table.Offset != 0 {
		query += fmt.Sprintf(" OFFSET %v", table.Offset)
	}
	return query
}

func buildCountQuery(table *abstract.TableDescription, deletable bool, additionalCond string) string {
	query := fmt.Sprintf("SELECT COUNT(*) - %d FROM `%s`.`%s` WHERE 1=1 ", table.Offset, table.Schema, table.Name)
	if table.Filter != "" {
		query += fmt.Sprintf(" AND (%s)", table.Filter)
	}
	if additionalCond != "" {
		query += fmt.Sprintf(" AND (%s)", additionalCond)
	}

	query += getDeleteTimeFilterExpr(deletable)
	return query
}
