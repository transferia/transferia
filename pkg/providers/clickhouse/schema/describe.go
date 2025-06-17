//go:build !disable_clickhouse_provider

package schema

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
)

func DescribeTable(db *sql.DB, database, table string, knownPrimaryKeys []string) (*abstract.TableSchema, error) {
	describeQ := fmt.Sprintf("DESCRIBE table `%v`.`%v`", database, table)
	describeRes, err := db.Query(describeQ)
	if err != nil {
		return nil, xerrors.Errorf("unable to describe table %v.%v: %w", database, table, err)
	}
	defer describeRes.Close()
	colTypes := make(map[string]string)
	colExpressions := make(map[string]string)
	colDefaultType := make(map[string]string)
	colPrimary := make(map[string]bool)
	var colNames []string

	for describeRes.Next() {
		resColNames, err := describeRes.Columns()
		if err != nil {
			return nil, xerrors.Errorf("unable to get describe table %v.%v result columns: %w", database, table, err)
		}
		resVals := make([]interface{}, len(resColNames))
		valsPtrs := make([]interface{}, len(resColNames))
		for i := 0; i < len(resColNames); i++ {
			valsPtrs[i] = &resVals[i]
		}
		if err := describeRes.Scan(valsPtrs...); err != nil {
			return nil, xerrors.Errorf(
				"unable to parse describe table %v.%v result into columns(%v): %w",
				database, table, resColNames, err)
		}

		parseColProperty := func(parsed *string, raw interface{}) bool {
			if v, ok := raw.(string); ok {
				*parsed = v
				return true
			}
			return false
		}
		var colname, coltype, expression, defaultType string
		for i, colProperty := range resColNames {
			if colProperty == "name" {
				if !parseColProperty(&colname, resVals[i]) {
					return nil, xerrors.Errorf(
						"unable to parse column 'name' from describe table %v.%v result: %v",
						database, table, resVals[i])
				}
			}

			if colProperty == "type" {
				if !parseColProperty(&coltype, resVals[i]) {
					return nil, xerrors.Errorf(
						"unable to parse column 'type' from describe table %v.%v result: %v",
						database, table, resVals[i])
				}
			}

			if colProperty == "default_expression" {
				if !parseColProperty(&expression, resVals[i]) {
					return nil, xerrors.Errorf(
						"unable to parse column 'default_expression' from describe table %v.%v result: %v",
						database, table, resVals[i])
				}
			}

			if colProperty == "default_type" {
				if !parseColProperty(&defaultType, resVals[i]) {
					return nil, xerrors.Errorf(
						"unable to parse column 'default_type' from describe table %v.%v result: %v",
						database, table, resVals[i])
				}
			}
		}
		colNames = append(colNames, colname)
		colTypes[colname] = coltype
		colExpressions[colname] = expression
		colDefaultType[colname] = defaultType
	}
	if err := describeRes.Err(); err != nil {
		return nil, xerrors.Errorf("unable to read describe rows: %w", err)
	}
	// clickhouse will return unordered columns, this need to set stable column names order
	sort.Strings(colNames)
	schema := make([]abstract.ColSchema, 0)
	// add primary keys first
	for _, pkeyColname := range knownPrimaryKeys {
		colPrimary[pkeyColname] = true
		coltype, ok := colTypes[pkeyColname]
		if !ok {
			// primary key has no column type, so it generated, skip it
			continue
		}

		ytType, required := columntypes.ToYtType(coltype)
		schema = append(schema, abstract.ColSchema{
			TableSchema:  database,
			TableName:    table,
			ColumnName:   pkeyColname,
			DataType:     ytType,
			PrimaryKey:   true,
			Required:     required,
			Expression:   fmt.Sprintf("%s:%s", colDefaultType[pkeyColname], colExpressions[pkeyColname]),
			OriginalType: fmt.Sprintf("ch:%s", coltype),
			Path:         "",
			FakeKey:      false,
			Properties:   nil,
		})
	}

	// add other fields
	for _, colname := range colNames {
		if colname == "__data_transfer_commit_time" || colname == "__data_transfer_delete_time" || colname == "__data_transfer_is_deleted" {
			continue
		}
		if colPrimary[colname] {
			continue
		}
		coltype := colTypes[colname]
		ytType, required := columntypes.ToYtType(coltype)
		schema = append(schema, abstract.ColSchema{
			TableSchema:  database,
			TableName:    table,
			ColumnName:   colname,
			DataType:     ytType,
			Required:     required,
			Expression:   fmt.Sprintf("%s:%s", colDefaultType[colname], colExpressions[colname]),
			OriginalType: fmt.Sprintf("ch:%s", coltype),
			Path:         "",
			PrimaryKey:   false,
			FakeKey:      false,
			Properties:   nil,
		})
	}
	return abstract.NewTableSchema(schema), nil
}
