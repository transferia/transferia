//go:build !disable_mysql_provider

package mysql

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	columnList = `
		select
			c.table_schema,
			c.table_name,
			c.column_name,
			c.column_type,
			c.collation_name
		from information_schema.columns c
		inner join information_schema.tables t
			on c.table_schema = t.table_schema
			and c.table_name = t.table_name
			and t.table_type in %s
		where c.table_schema not in ('sys', 'mysql', 'information_schema', 'performance_schema')
		order by c.table_name, c.column_name;
	`

	baseTablesOnly     = "('BASE TABLE')"
	baseTablesAndViews = "('BASE TABLE', 'VIEW')"

	constraintList = `
		select distinct
			table_schema,
			table_name,
			column_name,
			ordinal_position,
			constraint_name
		from
			information_schema.key_column_usage
		where
			table_schema not in ('sys', 'mysql', 'information_schema', 'performance_schema')
			and table_name in (
				select table_name from information_schema.tables
				where table_schema not in ('sys', 'mysql', 'information_schema', 'performance_schema')
				and table_type in ('BASE TABLE', 'VIEW')
			)
		order by
			table_schema,
			table_name,
			CONSTRAINT_NAME = 'PRIMARY' desc, -- Look at PRIMARY constraint than anything with ord_position
			ordinal_position
		;

	`

	tableConstraintList = `
                select distinct
                        table_schema,
                        table_name,
                        column_name,
                        ordinal_position,
                        constraint_name
                from
                        information_schema.key_column_usage
                where
                        table_schema = ?
                        and table_name = ?
                order by
                        table_schema,
                        table_name,
                        CONSTRAINT_NAME = 'PRIMARY' desc, -- Look at PRIMARY constraint than anything with ord_position
                        ordinal_position
                ;

        `

	expressionList = `
		select distinct
			table_schema,
			table_name,
			column_name,
			generation_expression
		from
			information_schema.columns
		where
			table_schema not in ('sys', 'mysql', 'information_schema', 'performance_schema')
			and table_name in (
				select table_name from information_schema.tables
				where table_schema not in ('sys', 'mysql', 'information_schema', 'performance_schema')
				and table_type in ('BASE TABLE', 'VIEW')
			)
			and generation_expression is not null and generation_expression != ''
		;
`
)

type queryExecutor interface {
	Query(sql string, args ...interface{}) (*sql.Rows, error)
}

func LoadSchema(tx queryExecutor, useFakePrimaryKey bool, includeViews bool) (abstract.DBSchema, error) {
	includeViewsSQL := baseTablesAndViews
	if !includeViews {
		includeViewsSQL = baseTablesOnly
	}
	rows, err := tx.Query(fmt.Sprintf(columnList, includeViewsSQL))
	if err != nil {
		msg := "unable to select column list"
		logger.Log.Error(msg, log.Error(err))
		return nil, xerrors.Errorf("%v: %w", msg, err)
	}
	tableCols := make(map[abstract.TableID]abstract.TableColumns)
	for rows.Next() {
		var col abstract.ColSchema
		var colTyp string
		var collation sql.NullString
		err := rows.Scan(
			&col.TableSchema,
			&col.TableName,
			&col.ColumnName,
			&colTyp,
			&collation,
		)
		if err != nil {
			msg := "unable to scan value from column list"
			logger.Log.Error(msg, log.Error(err))
			return nil, xerrors.Errorf("%v: %w", msg, err)
		}
		col.DataType = TypeToYt(colTyp).String()
		col.OriginalType = "mysql:" + colTyp
		if _, ok := tableCols[col.TableID()]; !ok {
			tableCols[col.TableID()] = make([]abstract.ColSchema, 0)
		}
		tableCols[col.TableID()] = append(tableCols[col.TableID()], col)
	}
	pKeys := make(map[abstract.TableID][]string)
	uKeys := make(map[abstract.TableID][]string)
	keyRows, err := tx.Query(constraintList)
	if err != nil {
		msg := "unable to select constraints"
		logger.Log.Error(msg, log.Error(err))
		return nil, xerrors.Errorf("%v: %w", msg, err)
	}
	for keyRows.Next() {
		var col abstract.ColSchema
		var pos int
		var constraintName sql.NullString
		err := keyRows.Scan(
			&col.TableSchema,
			&col.TableName,
			&col.ColumnName,
			&pos,
			&constraintName,
		)
		if err != nil {
			msg := "unable to scan constraint list"
			logger.Log.Error(msg, log.Error(err))
			return nil, xerrors.Errorf("%v: %w", msg, err)
		}
		if constraintName.Valid && constraintName.String == "PRIMARY" {
			pKeys[col.TableID()] = append(pKeys[col.TableID()], col.ColumnName)
		} else {
			uKeys[col.TableID()] = append(uKeys[col.TableID()], col.ColumnName)
		}
	}
	for tID, currSchema := range tableCols {
		keys := pKeys[tID]
		if len(keys) == 0 {
			keys = uKeys[tID]
		}
		tableSchema := makeTableSchema(currSchema, uniq(keys))

		if useFakePrimaryKey && !tableSchema.HasPrimaryKey() {
			for i := range tableSchema {
				tableSchema[i].PrimaryKey = true
				tableSchema[i].FakeKey = true
			}
		}
		tableCols[tID] = tableSchema
	}
	dbSchema := make(abstract.DBSchema)
	for tableID, columns := range enrichExpressions(tx, tableCols) {
		dbSchema[tableID] = abstract.NewTableSchema(columns)
	}
	return dbSchema, nil
}

func LoadTableConstraints(tx queryExecutor, table abstract.TableID) (map[string][]string, error) {
	constraints := make(map[string][]string)
	cRows, err := tx.Query(tableConstraintList, table.Namespace, table.Name)
	if err != nil {
		errMsg := fmt.Sprintf("cannot fetch constraints for table %v.%v", table.Namespace, table.Name)
		logger.Log.Errorf("%v: %v", errMsg, err)
		return constraints, xerrors.Errorf("%v: %w", errMsg, err)
	}
	for cRows.Next() {
		var col abstract.ColSchema
		var pos int
		var constraintName sql.NullString
		err := cRows.Scan(
			&col.TableSchema,
			&col.TableName,
			&col.ColumnName,
			&pos,
			&constraintName,
		)
		if err != nil {
			errMsg := fmt.Sprintf("unable to scan constraint list for table %v.%v", table.Namespace, table.Name)
			logger.Log.Errorf("%v: %v", errMsg, err)
			return constraints, xerrors.Errorf("%v: %w", errMsg, err)
		}
		if constraintName.Valid {
			cName := constraintName.String
			constraints[cName] = append(constraints[cName], col.ColumnName)
		}
	}
	return constraints, nil
}

func enrichExpressions(tx queryExecutor, schema map[abstract.TableID]abstract.TableColumns) map[abstract.TableID]abstract.TableColumns {
	rows, err := tx.Query(expressionList)
	if err != nil {
		logger.Log.Warnf("Unable to enrich expressions: %v", err)
		return schema
	}

	for rows.Next() {
		var col abstract.ColSchema
		var expr sql.NullString
		err := rows.Scan(
			&col.TableSchema,
			&col.TableName,
			&col.ColumnName,
			&expr)
		if err != nil {
			logger.Log.Warnf("Unable to enrich expressions: %v", err)
			return schema
		}
		if expr.Valid && expr.String != "" {
			for i := range schema[col.TableID()] {
				if schema[col.TableID()][i].ColumnName == col.ColumnName {
					schema[col.TableID()][i].Expression = expr.String
				}
			}
		}
	}
	return schema
}

func uniq(items []string) []string {
	h := map[string]bool{}
	var res []string
	for _, item := range items {
		if !h[item] {
			h[item] = true
			res = append(res, item)
		}
	}
	return res
}

func makeTableSchema(schemas []abstract.ColSchema, pkeys []string) abstract.TableColumns {
	pkeyMap := map[string]int{}
	for keySeqNumber, k := range pkeys {
		pkeyMap[k] = keySeqNumber
	}

	result := make(abstract.TableColumns, len(schemas))
	for i := range schemas {
		result[i] = schemas[i]
		_, result[i].PrimaryKey = pkeyMap[result[i].ColumnName]
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].PrimaryKey && !result[j].PrimaryKey })
	sort.SliceStable(result[:len(pkeys)], func(i, j int) bool { return pkeyMap[result[i].ColumnName] < pkeyMap[result[j].ColumnName] })

	return result
}
