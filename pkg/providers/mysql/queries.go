package mysql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
)

// explicitDefaultsForTimestamp enables standard SQL behavior for TIMESTAMP columns,
// allowing DEFAULT NULL on any TIMESTAMP column regardless of position in the table.
// Without this, MySQL 5.6/5.7 and MariaDB ≤10.5 apply non-standard legacy rules:
// the first TIMESTAMP column implicitly becomes NOT NULL DEFAULT CURRENT_TIMESTAMP,
// causing Error 1067 when DEFAULT NULL is explicitly specified.
// This setting is a no-op on MySQL 8.0+ / MariaDB 10.10+ where it's already the default.
const explicitDefaultsForTimestamp = "SET SESSION explicit_defaults_for_timestamp = ON;\n"

func prepareCreateTableQuery(tableID abstract.TableID, tableSchema *abstract.TableSchema) string {
	tModel := TemplateModel{
		Cols:  []TemplateCol{},
		Keys:  []TemplateCol{},
		Table: fmt.Sprintf("`%v`.`%v`", tableID.Namespace, tableID.Name),
	}
	for _, col := range tableSchema.Columns() {
		// Primary key columns are always NOT NULL — MySQL rejects DEFAULT NULL on them (Error 1171).
		required := col.Required || col.PrimaryKey
		tModel.Cols = append(tModel.Cols, TemplateCol{
			Name:     fmt.Sprintf("`%s`", col.ColumnName),
			Typ:      TypeToMySQL(col),
			Comma:    ",",
			Required: required,
		})
		if col.PrimaryKey {
			tModel.Keys = append(tModel.Keys, TemplateCol{
				Name:     fmt.Sprintf("`%s`", col.ColumnName),
				Typ:      TypeToMySQL(col),
				Comma:    ",",
				Required: false,
			})
		}
	}
	if len(tModel.Keys) > 0 {
		tModel.Keys[len(tModel.Keys)-1].Comma = ""
	} else {
		tModel.Cols[len(tModel.Cols)-1].Comma = ""
	}

	buf := new(bytes.Buffer)
	_ = createTableTemplate.Execute(buf, tModel)
	return explicitDefaultsForTimestamp + buf.String()
}

func prepareAlterTableQuery(tableID abstract.TableID, currentSchema, newSchema *abstract.TableSchema) string {
	existingColumns := currentSchema.FastColumns()
	var newColumns []string
	for _, col := range newSchema.Columns() {
		if _, ok := existingColumns[abstract.ColumnName(col.ColumnName)]; !ok {
			newColumns = append(newColumns, fmt.Sprintf("ADD COLUMN `%s` %s", col.ColumnName, TypeToMySQL(col)))
		}
	}

	if len(newColumns) == 0 {
		return ""
	}

	return fmt.Sprintf("ALTER TABLE `%s`.`%s` %s;",
		tableID.Namespace,
		tableID.Name,
		strings.Join(newColumns, ", "))
}
