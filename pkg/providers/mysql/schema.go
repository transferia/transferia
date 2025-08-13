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
			%s
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
				%s
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
				%s
			)
			and generation_expression is not null and generation_expression != ''
		;
`
)

type queryExecutor interface {
	Query(sql string, args ...interface{}) (*sql.Rows, error)
}

func LoadSchema(tx queryExecutor, useFakePrimaryKey bool, includeViews bool, database string) (abstract.DBSchema, error) {
	includeViewsSQL := baseTablesAndViews
	if !includeViews {
		includeViewsSQL = baseTablesOnly
	}
	query := fmt.Sprintf(columnList, includeViewsSQL, "")
	if database != "" {
		query = fmt.Sprintf(columnList, includeViewsSQL, fmt.Sprintf("and c.table_schema = '%s'", database))
	}
	rows, err := tx.Query(query)
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
	hasPrimaryKey := make(map[abstract.TableID]bool)
	tmpConstraintNames := make(map[abstract.TableID][]string)
	tmpColumnNames := make(map[abstract.TableID][]string)
	tmpColumnPositions := make(map[abstract.TableID][]int)
	pKeys := make(map[abstract.TableID][]string)
	uKeys := make(map[abstract.TableID][]string)
	query = fmt.Sprintf(constraintList, "")
	if database != "" {
		query = fmt.Sprintf(constraintList, fmt.Sprintf("and table_schema = '%s'", database))
	}
	keyRows, err := tx.Query(query)
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
			hasPrimaryKey[col.TableID()] = true
		} else {
			uKeys[col.TableID()] = append(uKeys[col.TableID()], col.ColumnName)
			tmpConstraintNames[col.TableID()] = append(tmpConstraintNames[col.TableID()], constraintName.String)
			tmpColumnNames[col.TableID()] = append(tmpColumnNames[col.TableID()], col.ColumnName)
			tmpColumnPositions[col.TableID()] = append(tmpColumnPositions[col.TableID()], pos)
		}
	}

	// TODO remove after TM-9031
	for tableID, ok := range hasPrimaryKey {
		if !ok {
			continue
		}

		logger.Log.Infof("DEBUG TM-9031: table %s.%s has no primary key on SNAPSHOT, constraints: %v, columns: %v, postitions: %v", tableID.Namespace, tableID.Name, tmpConstraintNames[tableID], tmpColumnNames[tableID], tmpColumnPositions[tableID])
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
	for tableID, columns := range enrichExpressions(tx, tableCols, database) {
		dbSchema[tableID] = abstract.NewTableSchema(columns)
	}
	return dbSchema, nil
}

func LoadTableConstraints(tx queryExecutor, table abstract.TableID) (map[string][]string, error) {
	hasPrimaryKey := false
	tmpConstraintNames := make([]string, 0)
	tmpColumnNames := make([]string, 0)
	tmpColumnPositions := make([]int, 0)

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

		if constraintName.Valid && constraintName.String == "PRIMARY" {
			hasPrimaryKey = true
		} else {
			tmpConstraintNames = append(tmpConstraintNames, constraintName.String)
			tmpColumnNames = append(tmpColumnNames, col.ColumnName)
			tmpColumnPositions = append(tmpColumnPositions, pos)
		}
	}

	// TODO remove after TM-9031
	if !hasPrimaryKey {
		logger.Log.Infof("DEBUG TM-9031: table %s.%s has no primary key on REPLICATION, constraints: %v, columns: %v, postitions: %v", table.Namespace, table.Name, tmpConstraintNames, tmpColumnNames, tmpColumnPositions)
	}

	return constraints, nil
}

func enrichExpressions(tx queryExecutor, schema map[abstract.TableID]abstract.TableColumns, database string) map[abstract.TableID]abstract.TableColumns {
	query := fmt.Sprintf(expressionList, "")
	if database != "" {
		query = fmt.Sprintf(expressionList, fmt.Sprintf("and table_schema = '%s'", database))
	}
	rows, err := tx.Query(query)
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
