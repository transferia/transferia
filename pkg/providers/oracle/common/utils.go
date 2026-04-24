//nolint:descriptiveerrors
package common

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
)

const (
	EmbeddedLogTrackerTableName = "data_transfer_tracker"
	CLOBToBLOBFunctionName      = "clob_to_blob"
)

type QueryFunc func(ctx context.Context, connection *sqlx.Conn) error

var (
	// From Oracle 11, 12, 19, 21
	// Oracle 11 - https://docs.oracle.com/cd/E11882_01/install.112/e47798/startrdb.htm#g1016156
	// Oracle 12 - https://docs.oracle.com/en/database/oracle/oracle-database/12.2/hpdbi/oracle-database-system-privileges-accounts-and-passwords.html
	// Oracle 19 - https://docs.oracle.com/en/database/oracle/oracle-database/19/ntdbi/oracle-database-system-privileges-accounts-and-passwords.html#GUID-7513171C-1055-48BB-8C79-B27EECC9B7E9
	// Oracle 21 - https://docs.oracle.com/en/database/oracle/oracle-database/21/dbseg/managing-security-for-oracle-database-users.html#GUID-D3770171-8E64-461C-92A4-045248EE42E1
	BannedUsers = []string{
		"ANONYMOUS",
		"APEX_040000",
		"APEX_050000",
		"APEX_PUBLIC_USER",
		"APPQOSSYS",
		"AUDSYS",
		"CTXSYS",
		"DBSFWUSER",
		"DBSNMP",
		"DBSNMP",
		"DGPDB_INT",
		"DIP",
		"DVF",
		"DVSYS",
		"EXFSYS",
		"FLOWS_040100",
		"FLOWS_FILES",
		"GGSYS",
		"GSMADMIN_INTERNAL",
		"GSMCATUSER",
		"GSMUSER",
		"LBACSYS",
		"MDDATA",
		"MDSYS",
		"MGMT_VIEW",
		"OLAPSYS",
		"ORACLE_OCM",
		"ORDDATA",
		"ORDPLUGINS",
		"ORDSYS",
		"OUTLN",
		"OWBSYS",
		"REMOTE_SCHEDULER_AGENT",
		"SI_INFORMTN_SCHEMA",
		"SPATIAL_CSW_ADMIN_USR",
		"SPATIAL_WFS_ADMIN_USR",
		"SYS",
		"SYS$UMF",
		"SYSBACKUP",
		"SYSDG",
		"SYSKM",
		"SYSMAN",
		"SYSMAN",
		"SYSRAC",
		"SYSTEM",
		"UNKNOWN",
		"WK_TEST",
		"WKPROXY",
		"WKSYS",
		"WMSYS",
		"XDB",
		"XS$NULL",
		"OJVMSYS",
	}
	BannedTables = []string{
		strings.ToUpper(EmbeddedLogTrackerTableName),
	}
)

func IsNullString(oracleString *string) bool {
	// Oracle treats an empty string as NULL
	return oracleString == nil || *oracleString == ""
}

func CreateSQLName(parts ...string) string {
	builder := strings.Builder{}
	for _, part := range parts {
		if builder.Len() > 0 {
			builder.WriteRune('.')
		}
		builder.WriteRune('"')
		builder.WriteString(part)
		builder.WriteRune('"')
	}
	return builder.String()
}

func ConvertOracleName(oracleName string) string {
	return strings.ToLower(oracleName)
}

func GetConnectionString(config *provider_oracle.OracleSource) (string, error) {
	oracleConnectionString := ""
	switch config.ConnectionType {
	case provider_oracle.OracleSIDConnection:
		oracleConnectionString = fmt.Sprintf("%s:%d/%s", config.Host, config.Port, config.SID)
	case provider_oracle.OracleServiceNameConnection:
		oracleConnectionString = fmt.Sprintf("%s:%d/%s", config.Host, config.Port, config.ServiceName)
	case provider_oracle.OracleTNSConnection:
		oracleConnectionString = config.TNSConnectString
		toDeleteSubStrings := []string{"\n", "\r", "\t"}
		for _, subString := range toDeleteSubStrings {
			oracleConnectionString = strings.ReplaceAll(oracleConnectionString, subString, "")
		}
	default:
		return "", xerrors.Errorf("Unsupported connection type '%v'", config.ConnectionType)
	}
	if strings.Contains(oracleConnectionString, "\"") {
		return "", xerrors.Errorf("Invalid connections string format '%s'", oracleConnectionString)
	}
	godrorConnectionString := fmt.Sprintf(
		`user="%s" password="%s" connectString="%s"`,
		config.User, config.Password, oracleConnectionString)

	return godrorConnectionString, nil
}

func getStringsInContent(values []string) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	builder := strings.Builder{}
	for _, value := range values {
		if builder.Len() > 0 {
			if _, err := builder.WriteString(", "); err != nil {
				return "", err
			}
		}
		builder.WriteRune('\'')
		builder.WriteString(value)
		builder.WriteRune('\'')
	}
	return builder.String(), nil
}

func GetBannedUsersInContent() (string, error) {
	return getStringsInContent(BannedUsers)
}

func GetBannedTablesInContent() (string, error) {
	return getStringsInContent(BannedTables)
}

func getInCondition(columnName string, inContent string, in bool) (string, error) {
	if columnName == "" {
		return "", xerrors.New("Column name is empty")
	}
	if inContent == "" {
		return "1 = 1", nil
	}
	if in {
		return fmt.Sprintf("%v in (%v)", columnName, inContent), nil
	} else {
		return fmt.Sprintf("%v not in (%v)", columnName, inContent), nil
	}
}

func GetBannedUsersCondition(userColumnName string) (string, error) {
	bannedUsersString, err := GetBannedUsersInContent()
	if err != nil {
		//nolint:descriptiveerrors
		return "", err
	}
	//nolint:descriptiveerrors
	return getInCondition(userColumnName, bannedUsersString, false)
}

func GetBannedTablesCondition(tableColumnName string) (string, error) {
	bannedTablesString, err := GetBannedTablesInContent()
	if err != nil {
		//nolint:descriptiveerrors
		return "", err
	}
	//nolint:descriptiveerrors
	return getInCondition(tableColumnName, bannedTablesString, false)
}

func GetTablesCondition(schemaColumnName string, tableColumnName string, tableIDs []*TableID, in bool) (string, error) {
	if schemaColumnName == "" {
		return "", xerrors.New("Schema column name is empty")
	}
	if tableColumnName == "" {
		return "", xerrors.New("Table column name is empty")
	}
	// dpiStmt_execute: ORA-01795: maximum number of expressions in a list is 1000
	var schemasCount, tablesCount int
	schemas := make([]string, 0)
	tables := make([]string, 0)
	schemasBuilder := strings.Builder{}
	tablesBuilder := strings.Builder{}

	for _, tableID := range tableIDs {
		if tableID.OracleTableName() == "*" {
			if schemasCount%1000 == 0 && schemasCount != 0 {
				schemas = append(schemas, schemasBuilder.String())
				schemasBuilder.Reset()
			}
			if schemasBuilder.Len() > 0 {
				schemasBuilder.WriteString(", ")
			}
			schemasBuilder.WriteRune('\'')
			schemasBuilder.WriteString(tableID.OracleSchemaName())
			schemasBuilder.WriteRune('\'')

			schemasCount++
		} else {
			if tablesCount%1000 == 0 && tablesCount != 0 {
				tables = append(tables, tablesBuilder.String())
				tablesBuilder.Reset()
			}
			if tablesBuilder.Len() > 0 {
				tablesBuilder.WriteString(", ")
			}
			tablesBuilder.WriteRune('\'')
			tablesBuilder.WriteString(tableID.OracleSQLName())
			tablesBuilder.WriteRune('\'')

			tablesCount++
		}
	}

	schemas = append(schemas, schemasBuilder.String())
	tables = append(tables, tablesBuilder.String())

	if len(schemas[0]) == 0 && len(tables[0]) == 0 {
		return "1 = 1", nil
	}

	filters := []string{}

	if len(schemas[0]) > 0 {
		for _, part := range schemas {
			filter, err := getInCondition(schemaColumnName, part, in)
			if err != nil {
				//nolint:descriptiveerrors
				return "", err
			}
			filters = append(filters, filter)
		}
	}

	if len(tables[0]) > 0 {
		for _, part := range tables {
			columnName := fmt.Sprintf("'\"'||%v||'\".\"'||%v||'\"'", schemaColumnName, tableColumnName)
			filter, err := getInCondition(columnName, part, in)
			if err != nil {
				//nolint:descriptiveerrors
				return "", err
			}
			filters = append(filters, filter)
		}
	}

	var splitter string
	if in {
		splitter = " or "
	} else {
		splitter = " and "
	}

	finalConditionBuilder := strings.Builder{}
	finalConditionBuilder.WriteString("(")
	for _, filter := range filters {
		if finalConditionBuilder.Len() > 1 {
			finalConditionBuilder.WriteString(splitter)
		}
		finalConditionBuilder.WriteString(filter)
	}
	finalConditionBuilder.WriteString(")")
	return finalConditionBuilder.String(), nil
}

func PDBQuery(sqlxDB *sqlx.DB, ctx context.Context, cdb bool, container string, query QueryFunc) error {
	connection, connectionErr := sqlxDB.Connx(context.Background())
	if connectionErr != nil {
		return xerrors.Errorf("Can't create connection: %w", connectionErr)
	}

	closeConnection := func(prevError error) error {
		if closeErr := connection.Close(); closeErr != nil {
			if prevError != nil {
				return xerrors.Errorf("Close connection error: %v, due query error: %w", closeErr.Error(), prevError)
			}
			return xerrors.Errorf("Can't close connection: %w", closeErr)
		}
		//nolint:descriptiveerrors
		return prevError
	}

	if cdb {
		_, containerErr := connection.ExecContext(ctx, fmt.Sprintf("alter session set container = %v", container))
		if containerErr != nil {
			//nolint:descriptiveerrors
			return closeConnection(xerrors.Errorf("Can't set PDB container to '%v': %w", container, containerErr))
		}
	}

	queryErr := query(ctx, connection)
	//nolint:descriptiveerrors
	return closeConnection(queryErr)
}

func CDBQuery(sqlxDB *sqlx.DB, ctx context.Context, cdb bool, query QueryFunc) error {
	return PDBQuery(sqlxDB, ctx, cdb, "cdb$root", query)
}

func PDBQueryGlobal(config *provider_oracle.OracleSource, sqlxDB *sqlx.DB, ctx context.Context, query QueryFunc) error {
	return PDBQuery(sqlxDB, ctx, config.CDB(), config.PDB, query)
}

func CDBQueryGlobal(config *provider_oracle.OracleSource, sqlxDB *sqlx.DB, ctx context.Context, query QueryFunc) error {
	return CDBQuery(sqlxDB, ctx, config.CDB(), query)
}
