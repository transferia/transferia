//go:build !disable_mysql_provider

package mysql

import (
	"bufio"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

const (
	keyMark        = "  KEY "
	constraintMark = "  CONSTRAINT "
	foreignKeyMark = " FOREIGN KEY "
)

var AlreadyExistsCodes = map[int]bool{
	1050: true, // For tables and views
	1304: true, // For procedures and functions
	1359: true, // For triggers
	1826: true, // For foreign keys
}

var TableOrViewNotExistsCode = uint16(1146)

func CopySchema(source *MysqlSource, steps *MysqlDumpSteps, pusher abstract.Pusher) error {
	storage, err := NewStorage(source.ToStorageParams())
	if err != nil {
		return xerrors.Errorf("unable to create mysql storage: %w", err)
	}
	defer storage.Close()

	databases, err := listDatabases(source, storage)
	if err != nil {
		return xerrors.Errorf("Cannot discover databases list to be transfered: %w", err)
	}

	connection := sqlx.NewDb(storage.DB, "mysql")
	if steps.Routine {
		routinesDDL, err := getRoutinesDDLs(connection, databases, storage.ConnectionParams.User)
		if err != nil {
			return xerrors.Errorf("unable to get routine ddl: %w", err)
		}
		err = applyDDLs(routinesDDL, pusher)
		if err != nil {
			return xerrors.Errorf("unable to apply Routine DDL: %w", err)
		}
	}

	if steps.Tables {
		tableDDLs, foreignKeyDDLs, err := GetTableDDLs(source, connection, databases)
		if err != nil {
			return xerrors.Errorf("unable to get table ddl: %w", err)
		}
		err = applyDDLs(tableDDLs, pusher)
		if err != nil {
			return xerrors.Errorf("unable to apply Table DDL: %w", err)
		}
		err = applyDDLs(foreignKeyDDLs, pusher)
		if err != nil {
			return xerrors.Errorf("unable to apply Foreign Key DDL: %w", err)
		}
	}

	if steps.View {
		viewDDLs, err := GetViewDDLs(source, connection, databases)
		if err != nil {
			return xerrors.Errorf("unable to get view ddl: %w", err)
		}
		err = applyDDLs(viewDDLs, pusher)
		if err != nil {
			return xerrors.Errorf("unable to apply View DDL: %w", err)
		}
	}

	if steps.Trigger {
		triggerDDLs, err := GetTriggerDDLs(source, connection, databases)
		if err != nil {
			return xerrors.Errorf("unable to get trigger ddl: %w", err)
		}
		err = applyDDLs(triggerDDLs, pusher)
		if err != nil {
			return xerrors.Errorf("unable to apply Trigger DDL: %w", err)
		}
	}

	return nil
}

type ddlValue struct {
	statement  string
	commitTime time.Time
	schema     string
	name       string
}

func applyDDLs(ddls []ddlValue, pusher abstract.Pusher) error {
	var toPush []*abstract.ChangeItem
	for _, ddl := range ddls {
		item := new(abstract.ChangeItem)
		item.Kind = abstract.DDLKind
		item.CommitTime = uint64(ddl.commitTime.UnixNano())
		item.ColumnValues = []interface{}{ddl.statement}
		item.Schema = ddl.schema
		item.Table = ddl.name
		if err := pusher([]abstract.ChangeItem{*item}); err != nil {
			if IsErrorCode(err, TableOrViewNotExistsCode) {
				toPush = append(toPush, item)
				continue
			}
			logger.Log.Warnf("Unable to apply ddl:\n%v\nerr:%v", ddl.statement, err)
			if !IsErrorCodes(err, AlreadyExistsCodes) {
				return xerrors.Errorf("Unable to apply ddl for table %v: %w", ddl.name, err)
			}
		}
	}

	for len(toPush) > 0 {
		var newToPush []*abstract.ChangeItem
		for _, item := range toPush {
			err := pusher([]abstract.ChangeItem{*item})
			if err == nil {
				continue
			}
			if IsErrorCode(err, TableOrViewNotExistsCode) {
				newToPush = append(newToPush, item)
				continue
			}
			logger.Log.Warnf("Unable to apply ddl:\n%v\nerr:%v", item.ColumnValues[0], err)
			if !IsErrorCodes(err, AlreadyExistsCodes) {
				return xerrors.Errorf("Unable to apply ddl for table %v: %w", item.Table, err)
			}
		}
		if len(toPush) == len(newToPush) {
			var statements, tableNames []string
			for _, item := range toPush {
				statements = append(statements, item.ColumnValues[0].(string))
				tableNames = append(tableNames, item.Table)
			}
			logger.Log.Warnf("Unable to apply ddls:\n%v", statements)
			return xerrors.Errorf("Unable to apply ddls for tables %v", tableNames)
		}
		toPush = newToPush
	}

	return nil
}

func filterTables(source *MysqlSource, tables []StatementID) []StatementID {
	var filteredTables []StatementID
	for _, table := range tables {
		if source.Include(abstract.TableID{Namespace: table.Namespace, Name: table.Name}) {
			filteredTables = append(filteredTables, table)
		}
	}
	return filteredTables
}

func listDatabases(source *MysqlSource, storage *Storage) ([]string, error) {
	allTables, err := storage.TableList(nil)
	if err != nil {
		return nil, xerrors.Errorf("Unable to list tables for inferring databases list: %w", err)
	}

	databases := make(map[string]bool)
	for table := range allTables {
		if source.Include(table) {
			databases[table.Namespace] = true
		}
	}

	dbList := make([]string, len(databases))
	i := 0
	for db := range databases {
		dbList[i] = db
		i++
	}
	return dbList, nil
}

func listToString(vals []string) string {
	res := "('"
	res += strings.Join(vals, "', '")
	res += "')"
	return res
}

type StatementID struct {
	Namespace string `db:"Namespace"`
	Name      string `db:"Name"`
}

type CreateTableRow struct {
	Name string `db:"Table"`
	DDL  string `db:"Create Table"`
}

func GetTableDDLs(source *MysqlSource, connection *sqlx.DB, databases []string) ([]ddlValue, []ddlValue, error) {
	tables := []StatementID{}
	err := connection.Select(&tables,
		fmt.Sprintf(
			"select table_schema as Namespace, table_name as Name from information_schema.tables where table_schema in %v and table_type != 'VIEW'",
			listToString(databases)))
	if err != nil {
		return nil, nil, xerrors.Errorf("Cannot list tables to discover create ddl: %w", err)
	}
	tables = filterTables(source, tables)
	tableDDLs := make([]ddlValue, 0)
	foreignKeyDDLs := make([]ddlValue, 0)
	for _, table := range tables {
		var output CreateTableRow
		err := connection.Get(&output, fmt.Sprintf("show create table `%v`.`%v`;", table.Namespace, table.Name))
		if err != nil {
			return nil, nil, err
		}
		tableDDL, tableForeignKeyDDLs := parseTableDDL(table, output.DDL)
		tableDDLs = append(tableDDLs, ddlValue{
			statement:  tableDDL,
			commitTime: time.Now(),
			schema:     table.Namespace,
			name:       table.Name,
		})
		for _, fk := range tableForeignKeyDDLs {
			foreignKeyDDLs = append(foreignKeyDDLs, ddlValue{
				statement:  fk,
				commitTime: time.Now(),
				schema:     table.Namespace,
				name:       table.Name,
			})
		}
	}
	return tableDDLs, foreignKeyDDLs, nil
}

func parseTableDDL(table StatementID, ddl string) (string, []string) {
	scanner := bufio.NewScanner(strings.NewReader(ddl))
	rows := make([]string, 0)
	for scanner.Scan() {
		rows = append(rows, scanner.Text())
	}

	fkes := make([]string, 0)
	body := make([]string, 0)
	foreignKeyNames := map[string]bool{}
	for _, row := range rows {
		if strings.Contains(row, foreignKeyMark) {
			if row[len(row)-1:] == "," {
				fkes = append(fkes, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s", table.Namespace, table.Name, row[:len(row)-1]))
			} else {
				fkes = append(fkes, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s", table.Namespace, table.Name, row))
			}

			foreignKeyMarkIndex := strings.Index(row, foreignKeyMark)
			foreignKeyName := row[len(constraintMark):foreignKeyMarkIndex]
			foreignKeyNames[foreignKeyName] = true
		} else {
			body = append(body, row)
		}
	}

	filteredBody := make([]string, 0)
	for _, row := range body {
		if strings.HasPrefix(row, keyMark) {
			keyColumnIndex := strings.Index(row, " (")
			if keyColumnIndex >= 0 {
				keyName := row[len(keyMark):keyColumnIndex]
				if foreignKeyNames[keyName] {
					continue
				}
			}
		}
		filteredBody = append(filteredBody, row)
	}

	n := len(filteredBody)
	m := len(filteredBody[n-2])
	// If penultimate DDL row ends in ',', kill this comma...
	if filteredBody[n-2][m-1] == ',' {
		// unless we are in a comment! (I'm so sorry)
		// or unless we are in a partition statement
		lastLine := strings.TrimSpace(filteredBody[n-1])
		if !strings.HasSuffix(lastLine, "*/") && !strings.HasPrefix(lastLine, "PARTITION") {
			filteredBody[n-2] = filteredBody[n-2][:m-1]
		}
	}
	ddlQ := strings.ReplaceAll(strings.Join(filteredBody, "\n"), "CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
	return ddlQ, fkes
}

type CreateViewRow struct {
	Name               string `db:"View"`
	DDL                string `db:"Create View"`
	CharacterSetClient string `db:"character_set_client"`
	Collation          string `db:"collation_connection"`
}

func GetViewDDLs(source *MysqlSource, connection *sqlx.DB, databases []string) ([]ddlValue, error) {
	views := []StatementID{}
	err := connection.Select(&views,
		fmt.Sprintf(
			"select table_schema as Namespace, table_name as Name from information_schema.tables where table_schema in %v and table_type = 'VIEW'",
			listToString(databases)))
	if err != nil {
		return nil, xerrors.Errorf("unable to select views info: %w", err)
	}
	views = filterTables(source, views)
	viewDDLs := make([]ddlValue, 0)
	for _, view := range views {
		var output CreateViewRow
		err := connection.Get(&output, fmt.Sprintf("show create view `%v`.`%v`;", view.Namespace, view.Name))
		if err != nil {
			return nil, xerrors.Errorf("unable to get create view statement: %w", err)
		}
		viewDDL, err := parseViewDDL(output.DDL)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse create view statement: %w", err)
		}
		viewDDLs = append(viewDDLs, ddlValue{
			statement:  viewDDL,
			commitTime: time.Now(),
			schema:     view.Namespace,
			name:       view.Name,
		})
	}
	return viewDDLs, nil
}

func parseViewDDL(ddl string) (string, error) {
	cutBegin := strings.Index(strings.ToUpper(ddl), " DEFINER")
	if cutBegin < 0 {
		return "", xerrors.Errorf("Can't find ' DEFINER' in create view ddl: %v", ddl)
	}
	cutEnd := strings.Index(strings.ToUpper(ddl), " VIEW")
	if cutEnd < 0 {
		return "", xerrors.Errorf("Can't find ' VIEW' in create view ddl: %v", ddl)
	}
	ddl = ddl[:cutBegin] + ddl[cutEnd:]
	ddl = strings.ReplaceAll(ddl, "CREATE", "CREATE OR REPLACE")
	return ddl, nil
}

type RoutineRow struct {
	Name   string
	Schema string
	Typ    string
}

func parseRoutineRow(values []interface{}) (*RoutineRow, error) {
	if len(values) != 3 {
		return nil, xerrors.Errorf("Unable to parse routine: expected 3 column but got %v(%v)", len(values), values)
	}
	schema, ok := values[0].([]byte)
	if !ok {
		return nil, xerrors.Errorf("Unable to parse routine schema from value: %v", values[0])
	}

	name, ok := values[1].([]byte)
	if !ok {
		return nil, xerrors.Errorf("Unable to parse routine name from value: %v", values[1])
	}

	typ, ok := values[2].([]byte)
	if !ok {
		return nil, xerrors.Errorf("Unable to parse routine type from value: %v", values[2])
	}
	return &RoutineRow{
		Name:   string(name),
		Schema: string(schema),
		Typ:    string(typ),
	}, nil
}

type CreateProcedureRow struct {
	Name                string `db:"Procedure"`
	Mode                string `db:"sql_mode"`
	DDL                 string `db:"Create Procedure"`
	CharacterSetClient  string `db:"character_set_client"`
	ConnectionCollation string `db:"collation_connection"`
	DBCollation         string `db:"Database Collation"`
}

type CreateFunctionRow struct {
	Name                string `db:"Function"`
	Mode                string `db:"sql_mode"`
	DDL                 string `db:"Create Function"`
	CharacterSetClient  string `db:"character_set_client"`
	ConnectionCollation string `db:"collation_connection"`
	DBCollation         string `db:"Database Collation"`
}

var NotEnoughProcPermissions = "You don't have enough permissions to get not null 'Create Function'/'Create Procedure' field. See mysql documentation ('SHOW CREATE PROCEDURE Statement') for details. user: %s, %s: %s.%s"

func getRoutinesDDLs(connection *sqlx.DB, databases []string, user string) ([]ddlValue, error) {
	rows, err := connection.Queryx(
		fmt.Sprintf(
			"select routine_schema, specific_name, routine_type from information_schema.routines where routine_schema in %v",
			listToString(databases)))
	if err != nil {
		return nil, xerrors.Errorf("Unable to list routines for databases %v: %w", databases, err)
	}
	defer rows.Close()
	var routines []RoutineRow
	for rows.Next() {
		values, err := rows.SliceScan()
		if err != nil {
			return nil, xerrors.Errorf("Unable to scan routines query result: %w", err)
		}
		routine, err := parseRoutineRow(values)
		if err != nil {
			return nil, xerrors.Errorf("Unable to parse routines query result: %w", err)
		}
		routines = append(routines, *routine)
	}

	routineDDLs := make([]ddlValue, 0)
	for _, routine := range routines {
		var output interface{}
		switch routine.Typ {
		case "PROCEDURE":
			output = new(CreateProcedureRow)
		case "FUNCTION":
			output = new(CreateFunctionRow)
		default:
			return nil, xerrors.Errorf("Unknown routine type: %v", routine.Typ)
		}
		err := connection.Get(output, fmt.Sprintf("show create %v `%v`.`%v`;", routine.Typ, routine.Schema, routine.Name))
		if err != nil {
			if strings.Contains(err.Error(), "name \"Create Procedure\": converting NULL to string is unsupported") {
				return nil, xerrors.Errorf(NotEnoughProcPermissions, user, routine.Typ, routine.Schema, routine.Name)
			}
			return nil, xerrors.Errorf("unable to get create routine statement: %w", err)
		}
		var routineDDL string
		switch specificOutput := output.(type) {
		case *CreateProcedureRow:
			routineDDL = specificOutput.DDL
		case *CreateFunctionRow:
			routineDDL = specificOutput.DDL
		default:
			return nil, xerrors.Errorf("Unknown routine type: %T", output)
		}
		routineDDL, err = parseRoutineDDL(&routine, routineDDL)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse create routine statement: %w", err)
		}
		routineDDLs = append(routineDDLs, ddlValue{
			statement:  routineDDL,
			commitTime: time.Now(),
			name:       routine.Name,
			schema:     routine.Schema,
		})
	}
	return routineDDLs, nil
}

func parseRoutineDDL(routine *RoutineRow, ddl string) (string, error) {
	definerCutBegin := strings.Index(strings.ToUpper(ddl), " DEFINER")
	if definerCutBegin < 0 {
		return "", xerrors.Errorf("Can't find ' DEFINER' in create routine ddl: %v", ddl)
	}
	definerCutEnd := strings.Index(strings.ToUpper(ddl), " "+routine.Typ)
	if definerCutEnd < 0 {
		return "", xerrors.Errorf("Can't find ' %v' in create routine ddl: %v", routine.Typ, ddl)
	}
	rest := ddl[definerCutEnd:]
	securityCutBegin := strings.Index(rest, "SQL SECURITY")
	if securityCutBegin > 0 {
		securityCutBegin = strings.LastIndex(rest[:securityCutBegin], "\n")
		if securityCutBegin < 0 {
			return "", xerrors.Errorf("Can't find 'SQL SECURITY' line start in create routine ddl: %v", ddl)
		}
		securityCutEnd := strings.Index(rest[(securityCutBegin+1):], "\n") + securityCutBegin + 1
		if securityCutEnd < 0 {
			return "", xerrors.Errorf("Can't find end of 'SQL SECURITY' in create routine ddl: %v", ddl)
		}
		ddl = ddl[:definerCutBegin] + rest[:securityCutBegin] + rest[securityCutEnd:]
	} else {
		ddl = ddl[:definerCutBegin] + rest
	}
	ddl = fmt.Sprintf("drop %v if exists `%v`;\n", routine.Typ, routine.Name) + ddl
	return ddl, nil
}

type CreateTriggerRow struct {
	Name                string `db:"Trigger"`
	Mode                string `db:"sql_mode"`
	DDL                 string `db:"SQL Original Statement"`
	CharacterSetClient  string `db:"character_set_client"`
	ConnectionCollation string `db:"collation_connection"`
	DBCollation         string `db:"Database Collation"`
	Created             string `db:"Created"`
}

type TriggerRow struct {
	Namespace string
	Name      string
}

func parseTriggerRow(values []interface{}) (*TriggerRow, error) {
	if len(values) != 2 {
		return nil, xerrors.Errorf("Unable to parse trigger: expected 2 columns but got %v(%v)", len(values), values)
	}
	schema, ok := values[0].([]byte)
	if !ok {
		return nil, xerrors.Errorf("Unable to parse trigger schema from value: %v", values[0])
	}
	name, ok := values[1].([]byte)
	if !ok {
		return nil, xerrors.Errorf("Unable to parse trigger name from value: %v", values[1])
	}
	return &TriggerRow{
		Namespace: string(schema),
		Name:      string(name),
	}, nil
}

func GetTriggerDDLs(source *MysqlSource, connection *sqlx.DB, databases []string) ([]ddlValue, error) {
	rows, err := connection.Queryx(
		fmt.Sprintf(
			"select trigger_schema, trigger_name from information_schema.triggers where trigger_schema in %v",
			listToString(databases)))
	if err != nil {
		return nil, xerrors.Errorf("unable to select triggers info: %w", err)
	}
	defer rows.Close()

	var triggers []TriggerRow
	for rows.Next() {
		values, err := rows.SliceScan()
		if err != nil {
			return nil, xerrors.Errorf("Unable to scan trigger row: %w", err)
		}
		trigger, err := parseTriggerRow(values)
		if err != nil {
			return nil, xerrors.Errorf("Unable to parse trigger row: %w", err)
		}
		triggers = append(triggers, *trigger)
	}
	var triggerDDLs []ddlValue
	for _, trigger := range triggers {
		var output CreateTriggerRow
		err := connection.Get(&output, fmt.Sprintf("show create trigger `%v`.`%v`;", trigger.Namespace, trigger.Name))
		if err != nil {
			return nil, xerrors.Errorf("unable to get create trigger statement: %w", err)
		}
		triggerDDL, err := parseTriggerDDL(trigger, output.DDL)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse create trigger statement: %w", err)
		}
		triggerDDLs = append(triggerDDLs, ddlValue{
			statement:  triggerDDL,
			commitTime: time.Now(),
			name:       trigger.Name,
			schema:     trigger.Namespace,
		})
	}
	return triggerDDLs, nil
}

func parseTriggerDDL(trigger TriggerRow, ddl string) (string, error) {
	cutBegin := strings.Index(strings.ToUpper(ddl), " DEFINER")
	if cutBegin < 0 {
		return "", xerrors.Errorf("Can't find ' DEFINER' in create trigger ddl: %v", ddl)
	}
	cutEnd := strings.Index(strings.ToUpper(ddl), " TRIGGER")
	if cutEnd < 0 {
		return "", xerrors.Errorf("Can't find ' TRIGGER' in create trigger ddl: %v", ddl)
	}
	ddl = ddl[:cutBegin] + ddl[cutEnd:]
	ddl = fmt.Sprintf("drop trigger if exists `%v`;\n", trigger.Name) + ddl
	return ddl, nil
}
