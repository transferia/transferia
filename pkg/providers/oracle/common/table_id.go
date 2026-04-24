package common

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type TableID struct {
	schemaName string
	tableName  string
}

func NewTableID(schemaName string, tableName string) *TableID {
	return &TableID{
		schemaName: schemaName,
		tableName:  tableName,
	}
}

func NewTableIDsFromOracleSQLNames(sqlNames []string, user string) ([]*TableID, error) {
	tableIDs := make([]*TableID, len(sqlNames))
	for i, sqlName := range sqlNames {
		tableID, err := NewTableIDFromOracleSQLName(sqlName, user)
		if err != nil {
			return nil, xerrors.Errorf("Can't parse table name '%v': %w", sqlName, err)
		}
		tableIDs[i] = tableID
	}
	return tableIDs, nil
}

func NewTableIDFromOracleSQLName(oracleSQL string, user string) (*TableID, error) {
	parts := strings.Split(oracleSQL, ".")
	if len(parts) > 2 {
		return nil, xerrors.Errorf("Can't parse name '%v'", oracleSQL)
	}
	if len(parts) == 1 {
		schema := convertToStrictName(user)
		table := convertToStrictName(parts[0])
		return NewTableID(schema, table), nil
	} else if len(parts) == 2 {
		schema := convertToStrictName(parts[0])
		table := convertToStrictName(parts[1])
		return NewTableID(schema, table), nil
	} else {
		return nil, xerrors.Errorf("Can't parse name '%v'", oracleSQL)
	}
}

func convertToStrictName(name string) string {
	const nameQuote = "\""
	if !strings.HasPrefix(name, "\"") || !strings.HasSuffix(name, "\"") {
		return strings.ToUpper(name)
	}
	nameQuoteLen := len(nameQuote)
	name = name[nameQuoteLen : len(name)-nameQuoteLen]
	return name
}

func (table *TableID) OracleSchemaName() string {
	return table.schemaName
}

func (table *TableID) SchemaName() string {
	return ConvertOracleName(table.OracleSchemaName())
}

func (table *TableID) OracleTableName() string {
	return table.tableName
}

func (table *TableID) TableName() string {
	return ConvertOracleName(table.OracleTableName())
}

func (table *TableID) OracleSQLName() string {
	return CreateSQLName(table.OracleSchemaName(), table.OracleTableName())
}

func (table *TableID) FullName() string {
	return CreateSQLName(table.SchemaName(), table.TableName())
}

func (table *TableID) Equals(otherTableID *TableID) bool {
	return *table == *otherTableID
}

func (table *TableID) ToOldTableID() (*abstract.TableID, error) {
	return &abstract.TableID{
		Namespace: table.SchemaName(),
		Name:      table.TableName(),
	}, nil
}
