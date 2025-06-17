//go:build !disable_mysql_provider

package mysql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
)

func prepareCreateTableQuery(tableID abstract.TableID, tableSchema *abstract.TableSchema) string {
	tModel := TemplateModel{
		Cols:  []TemplateCol{},
		Keys:  []TemplateCol{},
		Table: fmt.Sprintf("`%v`.`%v`", tableID.Namespace, tableID.Name),
	}
	for _, col := range tableSchema.Columns() {
		tModel.Cols = append(tModel.Cols, TemplateCol{
			Name:  fmt.Sprintf("`%s`", col.ColumnName),
			Typ:   TypeToMySQL(col),
			Comma: ",",
		})
		if col.PrimaryKey {
			tModel.Keys = append(tModel.Keys, TemplateCol{
				Name:  fmt.Sprintf("`%s`", col.ColumnName),
				Typ:   TypeToMySQL(col),
				Comma: ",",
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
	return buf.String()
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
