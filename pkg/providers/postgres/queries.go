package postgres

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func CreateTableQuery(fullTableName string, schema []abstract.ColSchema) (string, error) {
	if err := prepareOriginalTypes(schema); err != nil {
		return "", xerrors.Errorf("failed to prepare original types for parsing: %w", err)
	}

	var primaryKeys []string
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (`, fullTableName))
	for idx, col := range schema {
		var queryType string
		if col.OriginalType != "" {
			queryType = strings.TrimPrefix(col.OriginalType, "pg:")
			queryType = strings.ReplaceAll(queryType, "USER-DEFINED", "TEXT")
		} else {
			var err error
			queryType, err = DataToOriginal(col.DataType)
			if err != nil {
				return "", xerrors.Errorf("failed to convert column %q to original type: %w", col.ColumnName, err)
			}
		}
		if strings.HasPrefix(col.Expression, "pg:") {
			queryType += " " + strings.TrimPrefix(col.Expression, "pg:")
		}
		if col.Required {
			queryType += " NOT NULL"
		}
		b.WriteString(fmt.Sprintf(`"%v" %v`, col.ColumnName, queryType))
		if col.IsKey() {
			primaryKeys = append(primaryKeys, fmt.Sprintf(`"%s"`, col.ColumnName))
		}
		if idx < len(schema)-1 {
			b.WriteString(",")
		}
	}
	if len(primaryKeys) > 0 {
		b.WriteString(fmt.Sprintf(", primary key (%v)", strings.Join(primaryKeys, ",")))
	}
	b.WriteString(")")

	return b.String(), nil
}

func createEnumQuery(col abstract.ColSchema) (string, error) {
	col, err := prepareOriginalType(col)
	if err != nil {
		return "", xerrors.Errorf("createEnumQuery:failed to prepare original types for parsing: %w", err)
	}
	typeName, err := strconv.Unquote(strings.TrimPrefix(col.OriginalType, "pg:"))
	if err != nil {
		return "", xerrors.Errorf("createEnumQuery:failed to unquote %s: %w", col.OriginalType, err)
	}
	allVals := GetPropertyEnumAllValues(&col)

	buf := strings.Builder{}
	for i, v := range allVals {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("'%s'", v))
	}
	return fmt.Sprintf(`
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pg_type
        WHERE
            typname = '%s') THEN
    CREATE TYPE "%s" AS ENUM (%s);
	END IF;
END;
$$;`, typeName, typeName, buf.String()), nil
}

func addEnumValsQuery(currentCol, col abstract.ColSchema) ([]string, error) {
	col, err := prepareOriginalType(col)
	if err != nil {
		return nil, xerrors.Errorf("addColsQuery:failed to prepare original types for parsing: %w", err)
	}
	currValsMap := make(map[string]int)
	currVals := GetPropertyEnumAllValues(&currentCol)
	for i, val := range currVals {
		currValsMap[val] = i
	}

	newValsMap := make(map[string]int)
	newVals := GetPropertyEnumAllValues(&col)
	for i, val := range newVals {
		newValsMap[val] = i
	}
	res := make([]string, 0, len(newVals))

	var toAdd []struct {
		val    string
		before string
	}
	var toRemove []string

	for i, val := range newVals {
		if _, exists := currValsMap[val]; !exists {
			var before string
			if i < len(newVals)-1 && len(currVals) > 0 {
				before = newVals[i+1]
			}
			toAdd = append(toAdd, struct {
				val    string
				before string
			}{val, before})
		}
	}

	for val := range currValsMap {
		if _, exists := newValsMap[val]; !exists {
			toRemove = append(toRemove, val)
		}
	}

	if len(toAdd) == 0 && len(toRemove) == 0 {
		return nil, nil
	}

	typeName := strings.TrimPrefix(col.OriginalType, "pg:")

	for _, add := range toAdd {

		if add.before != "" {
			res = append(res, fmt.Sprintf(`ALTER TYPE %s ADD VALUE IF NOT EXISTS '%s' BEFORE '%s'`,
				typeName, add.val, add.before))
		} else {
			res = append(res, fmt.Sprintf(`ALTER TYPE %s ADD VALUE IF NOT EXISTS '%s'`,
				typeName, add.val))
		}
	}

	for _, val := range toRemove {
		res = append(res, fmt.Sprintf(`ALTER TYPE %s DROP VALUE IF EXISTS '%s'`, typeName, val))
	}

	return res, nil
}

func addColsQuery(ftn string, added []abstract.ColSchema) (string, error) {
	if err := prepareOriginalTypes(added); err != nil {
		return "", xerrors.Errorf("addColsQuery:failed to prepare original types for parsing: %w", err)
	}

	b := strings.Builder{}
	b.WriteString(fmt.Sprintf(`ALTER TABLE %s `, ftn))
	for idx, col := range added {
		var queryType string
		if col.OriginalType != "" {
			queryType = strings.TrimPrefix(col.OriginalType, "pg:")
			queryType = strings.ReplaceAll(queryType, "USER-DEFINED", "TEXT")
		} else {
			var err error
			queryType, err = DataToOriginal(col.DataType)
			if err != nil {
				return "", xerrors.Errorf("addColsQuery:failed to convert column %q to original type: %w", col.ColumnName, err)
			}
		}
		if strings.HasPrefix(col.Expression, "pg:") {
			queryType += " " + strings.TrimPrefix(col.Expression, "pg:")
		}
		if col.Required {
			queryType += " NOT NULL"
		}
		b.WriteString(fmt.Sprintf(`ADD COLUMN IF NOT EXISTS "%v" %v`, col.ColumnName, queryType))
		if idx < len(added)-1 {
			b.WriteString(",")
		}
	}
	return b.String(), nil
}
