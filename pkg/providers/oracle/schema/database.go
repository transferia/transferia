package schema

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type Database struct {
	sqlxDB *sqlx.DB
	config *provider_oracle.OracleSource
	logger log.Logger

	// Metadata
	name                     string
	uniqueName               string
	role                     string
	clobToBLOBFunctionExists bool

	// Tables
	schemas      []*Schema
	schemaByName map[string]*Schema
}

func NewDatabase(sqlxDB *sqlx.DB, config *provider_oracle.OracleSource, logger log.Logger) (*Database, error) {
	//nolint:exhaustivestruct
	schemaRepository := &Database{
		sqlxDB:       sqlxDB,
		config:       config,
		logger:       logger,
		schemas:      []*Schema{},
		schemaByName: map[string]*Schema{},
	}
	return schemaRepository, nil
}

func (db *Database) Config() *provider_oracle.OracleSource {
	return db.config
}

func (db *Database) Name() string {
	return db.name
}

func (db *Database) FullName() string {
	return fmt.Sprintf("%v (%v, %v)", db.name, db.uniqueName, db.role)
}

func (db *Database) CLOBToBLOBFunctionExists() bool {
	return db.clobToBLOBFunctionExists
}

func (db *Database) CLOBAsBLOB() (bool, error) {
	switch db.Config().CLOBReadingStrategy {
	case provider_oracle.OracleReadCLOB:
		return false, nil
	case provider_oracle.OracleReadCLOBAsBLOB:
		return true, nil
	case provider_oracle.OracleReadCLOBAsBLOBIfFunctionExists:
		if db.CLOBToBLOBFunctionExists() {
			return true, nil
		}
		return false, nil
	default:
		return false, abstract.NewFatalError(
			xerrors.Errorf("Unsupported CLOB reading strategy type: '%v'", db.Config().CLOBReadingStrategy))
	}
}

func (db *Database) SchemasCount() int {
	return len(db.schemas)
}

func (db *Database) OracleSchema(i int) *Schema {
	return db.schemas[i]
}

func (db *Database) OracleSchemaByName(name string) *Schema {
	return db.schemaByName[name]
}

func (db *Database) OracleTableByID(tableID *oracle_common.TableID) *Table {
	schema := db.OracleSchemaByName(tableID.OracleSchemaName())
	if schema == nil {
		return nil
	}
	return schema.OracleTableByName(tableID.OracleTableName())
}

func (db *Database) add(schema *Schema) error {
	_, findOracleName := db.schemaByName[schema.OracleName()]
	_, findName := db.schemaByName[schema.Name()]
	if findOracleName || findName {
		return xerrors.Errorf("Duplicate schema '%v'", schema.OracleSQLName())
	}
	db.schemaByName[schema.OracleName()] = schema
	db.schemaByName[schema.Name()] = schema
	db.schemas = append(db.schemas, schema)
	return nil
}

func (db *Database) remove(schemaName string) {
	var schema *Schema
	for i := 0; i < len(db.schemas); i++ {
		if schemaName == db.schemas[i].OracleName() || schemaName == db.schemas[i].Name() {
			schema = db.schemas[i]
			db.schemas[i] = db.schemas[len(db.schemas)-1]
			db.schemas = db.schemas[:len(db.schemas)-1]
			break
		}
	}
	if schema == nil {
		return
	}
	delete(db.schemaByName, schema.OracleName())
	delete(db.schemaByName, schema.Name())
}

func (db *Database) addTable(tableID *oracle_common.TableID) (*Table, error) {
	schema := db.OracleSchemaByName(tableID.OracleSchemaName())
	if schema == nil {
		schema = NewSchema(db, tableID.OracleSchemaName())
		if err := db.add(schema); err != nil {
			//nolint:descriptiveerrors
			return nil, err
		}
	}
	table := schema.OracleTableByName(tableID.OracleTableName())
	if table == nil {
		table = NewTable(schema, tableID.OracleTableName())
		if err := schema.add(table); err != nil {
			//nolint:descriptiveerrors
			return nil, err
		}
	}
	return table, nil
}

func (db *Database) removeTable(tableID *oracle_common.TableID) {
	schema := db.OracleSchemaByName(tableID.OracleSchemaName())
	if schema == nil {
		return
	}
	schema.remove(tableID.OracleTableName())
	if schema.TablesCount() == 0 {
		db.remove(schema.Name())
	}
}

type NamesRow struct {
	Name       string `db:"NAME"`
	UniqueName string `db:"DB_UNIQUE_NAME"`
	Role       string `db:"DATABASE_ROLE"`
}

func (db *Database) loadNames() error {
	return oracle_common.PDBQueryGlobal(db.config, db.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			sql := "select NAME, DB_UNIQUE_NAME, DATABASE_ROLE from V$DATABASE"
			var row NamesRow
			err := connection.GetContext(ctx, &row, sql)
			if err != nil {
				return xerrors.Errorf("Can't select names: %w", err)
			}

			db.name = row.Name
			db.uniqueName = row.UniqueName
			db.role = row.Role

			return nil
		})
}

func (db *Database) loadCLOBToBLOBFunctionExists() error {
	return oracle_common.PDBQueryGlobal(db.config, db.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			sql := "select count(*) from ALL_SOURCE where OWNER = upper(:user_name) and NAME = upper(:function_name)"
			var count int
			err := connection.GetContext(ctx, &count, sql, db.config.User, oracle_common.CLOBToBLOBFunctionName)
			if err != nil {
				return xerrors.Errorf("Can't select is CLOB to BLOB exists: %w", err)
			}

			db.clobToBLOBFunctionExists = (count > 0)

			return nil
		})
}

func (db *Database) LoadMetadata() error {
	if err := db.loadNames(); err != nil {
		return xerrors.Errorf("Can't select names: %w", err)
	}

	if db.Config().CLOBReadingStrategy == provider_oracle.OracleReadCLOBAsBLOBIfFunctionExists {
		if err := db.loadCLOBToBLOBFunctionExists(); err != nil {
			return xerrors.Errorf("Can't select is CLOB to BLOB function exists: %w", err)
		}
	}

	return nil
}

//nolint:descriptiveerrors
func (db *Database) getDefaultTablesCondition(
	schemaColumnName string,
	tableColumnName string,
	includeTables []*oracle_common.TableID,
	excludeTables []*oracle_common.TableID,
) (string, error) {
	bannedUsersCondition, err := oracle_common.GetBannedUsersCondition(schemaColumnName)
	if err != nil {
		return "", err
	}
	bannedTablesCondition, err := oracle_common.GetBannedTablesCondition(tableColumnName)
	if err != nil {
		return "", err
	}
	includeTablesCondition, err := oracle_common.GetTablesCondition(schemaColumnName, tableColumnName, includeTables, true)
	if err != nil {
		return "", err
	}
	excludeTablexCondition, err := oracle_common.GetTablesCondition(schemaColumnName, tableColumnName, excludeTables, false)
	if err != nil {
		return "", err
	}
	condition := fmt.Sprintf("(%v and %v and %v and %v)",
		bannedUsersCondition, bannedTablesCondition, includeTablesCondition, excludeTablexCondition)
	return condition, nil
}

func (db *Database) LoadAllTables() error {
	db.schemas = []*Schema{}
	db.schemaByName = map[string]*Schema{}
	if err := db.loadTables(nil, nil); err != nil {
		//nolint:descriptiveerrors
		return err
	}
	return nil
}

type TableIDRow struct {
	SchemaName string `db:"OWNER"`
	TableName  string `db:"TABLE_NAME"`
}

func (db *Database) loadTableIDsFromConfig() ([]*oracle_common.TableID, error) {
	userIncludeTables, err := oracle_common.NewTableIDsFromOracleSQLNames(db.config.IncludeTables, db.config.User)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	userExcludeTables, err := oracle_common.NewTableIDsFromOracleSQLNames(db.config.ExcludeTables, db.config.User)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	defaultCondition, err := db.getDefaultTablesCondition("OWNER", "TABLE_NAME", userIncludeTables, userExcludeTables)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	sql := fmt.Sprintf(`
select
	OWNER, TABLE_NAME
from ALL_TABLES
where
	%v
	and TEMPORARY = 'N' and SECONDARY = 'N' and STATUS = 'VALID' and DROPPED = 'NO'`,
		defaultCondition)

	logger.Log.Info("Retrieving schema", log.String("query", util.Sample(sql, 10*1024)))

	tableIDs := []*oracle_common.TableID{}
	queryErr := oracle_common.PDBQueryGlobal(db.config, db.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			rows, err := connection.QueryxContext(ctx, sql)
			if err != nil {
				return xerrors.Errorf("Can't get tables from DB: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var row TableIDRow
				if err := rows.StructScan(&row); err != nil {
					return xerrors.Errorf("Can't parse row from DB: %w", err)
				}
				tableIDs = append(tableIDs, oracle_common.NewTableID(row.SchemaName, row.TableName))
			}

			if rows.Err() != nil {
				return xerrors.Errorf("Can't read row from DB: %w", rows.Err())
			}

			return nil
		})

	//nolint:descriptiveerrors
	return tableIDs, queryErr
}

func (db *Database) LoadTablesFromConfig() error {
	tableIDs, err := db.loadTableIDsFromConfig()
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}
	if len(tableIDs) == 0 {
		return xerrors.Errorf("There no tables")
	}

	db.schemas = []*Schema{}
	db.schemaByName = map[string]*Schema{}
	if err := db.loadTables(tableIDs, nil); err != nil {
		//nolint:descriptiveerrors
		return err
	}
	return nil
}

func (db *Database) LoadTable(tableID *oracle_common.TableID) error {
	db.removeTable(tableID)
	if err := db.loadTables([]*oracle_common.TableID{tableID}, nil); err != nil {
		//nolint:descriptiveerrors
		return err
	}
	return nil
}

func (db *Database) LoadTables(tableIDs []*oracle_common.TableID, in bool) error {
	for _, tableID := range tableIDs {
		db.removeTable(tableID)
	}
	if err := db.loadTables(tableIDs, nil); err != nil {
		//nolint:descriptiveerrors
		return err
	}
	return nil
}

func (db *Database) loadTables(includeTables []*oracle_common.TableID, excludeTables []*oracle_common.TableID) error {
	if err := db.loadColumns(includeTables, excludeTables); err != nil {
		return xerrors.Errorf("Can't load columns from DB: %w", err)
	}

	if err := db.loadPrimaryKeys(includeTables, excludeTables); err != nil {
		return xerrors.Errorf("Can't load primary keys from DB: %w", err)
	}
	if db.config.UseUniqueIndexesAsKeys {
		if err := db.loadUniqueIndexes(includeTables, excludeTables); err != nil {
			return xerrors.Errorf("Can't load unique indexes from DB: %w", err)
		}
	}

	if len(includeTables) > 0 {
		for _, tableID := range includeTables {
			table := db.OracleTableByID(tableID)
			if table == nil {
				continue
			}
			table.selectKeyIndex()
		}
	} else {
		for i := 0; i < db.SchemasCount(); i++ {
			schema := db.OracleSchema(i)
			for j := 0; j < schema.TablesCount(); j++ {
				table := schema.OracleTable(j)
				table.selectKeyIndex()
			}
		}
	}
	return nil
}

type ColumnRow struct {
	SchemaName    string  `db:"OWNER"`
	TableName     string  `db:"TABLE_NAME"`
	ColumnName    string  `db:"COLUMN_NAME"`
	DataType      *string `db:"DATA_TYPE"`
	DataLength    int     `db:"DATA_LENGTH"`
	DataPrecision *int    `db:"DATA_PRECISION"`
	DataScale     *int    `db:"DATA_SCALE"`
	Nullable      *string `db:"NULLABLE"`
	Virtual       *string `db:"VIRTUAL_COLUMN"`
}

func (db *Database) loadColumns(includeTables []*oracle_common.TableID, excludeTables []*oracle_common.TableID) error {
	defaultCondition, err := db.getDefaultTablesCondition("cols.OWNER", "cols.TABLE_NAME", includeTables, excludeTables)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}

	sql := fmt.Sprintf(`
select
	cols.OWNER, cols.TABLE_NAME, cols.COLUMN_NAME,
	cols.DATA_TYPE, cols.DATA_LENGTH, cols.DATA_PRECISION, cols.DATA_SCALE,
	cols.NULLABLE, cols.VIRTUAL_COLUMN
from ALL_TAB_COLS cols
where
	%v
	and cols.HIDDEN_COLUMN != 'YES'`,
		defaultCondition)

	logger.Log.Info("Loading columns info", log.String("query", util.Sample(sql, 10*1024)))
	//nolint:descriptiveerrors
	return oracle_common.PDBQueryGlobal(db.config, db.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			rows, err := connection.QueryxContext(ctx, sql)
			if err != nil {
				return xerrors.Errorf("Can't get columns from DB: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var row ColumnRow
				if err := rows.StructScan(&row); err != nil {
					return xerrors.Errorf("Can't parse row from DB: %w", err)
				}
				if err := db.addColumn(&row); err != nil {
					return xerrors.Errorf("Can't create column '%v' from DB: %w",
						oracle_common.CreateSQLName(row.SchemaName, row.TableName, row.ColumnName), err)
				}
			}

			if rows.Err() != nil {
				return xerrors.Errorf("Can't read row from DB: %w", rows.Err())
			}

			return nil
		})
}

func (db *Database) addColumn(row *ColumnRow) error {
	tableID := oracle_common.NewTableID(row.SchemaName, row.TableName)
	table := db.OracleTableByID(tableID)
	if table == nil {
		var err error
		table, err = db.addTable(tableID)
		if err != nil {
			//nolint:descriptiveerrors
			return err
		}
	}
	column, err := NewColumn(table, row, db.config.ConvertNumberToInt64)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}
	if err := table.addColumn(column); err != nil {
		//nolint:descriptiveerrors
		return err
	}
	return nil
}

func (db *Database) loadPrimaryKeys(includeTables []*oracle_common.TableID, excludeTables []*oracle_common.TableID) error {
	defaultCondition, err := db.getDefaultTablesCondition("cols.OWNER", "cols.TABLE_NAME", includeTables, excludeTables)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}

	sql := fmt.Sprintf(`
select cols.CONSTRAINT_NAME as INDEX_NAME, cols.OWNER, cols.TABLE_NAME, cols.COLUMN_NAME
from ALL_CONS_COLUMNS cols
inner join ALL_CONSTRAINTS cons
	on cons.OWNER = cols.OWNER and cons.TABLE_NAME = cols.TABLE_NAME and cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
where
	%v
	and cons.BAD is NULL
	and cons.INVALID is NULL
	and cons.CONSTRAINT_TYPE = 'P'
	and cons.STATUS = 'ENABLED'
	and EXISTS(select * from ALL_TABLES tabs where tabs.OWNER = cols.OWNER and tabs.TABLE_NAME = cols.TABLE_NAME)`,
		defaultCondition)

	//nolint:descriptiveerrors
	return db.loadIndexes(sql, IndexTypePrimaryKey)
}

func (db *Database) loadUniqueIndexes(includeTables []*oracle_common.TableID, excludeTables []*oracle_common.TableID) error {
	defaultCondition, err := db.getDefaultTablesCondition("cols.OWNER", "cols.TABLE_NAME", includeTables, excludeTables)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}

	sql := fmt.Sprintf(`
select cols.INDEX_NAME, cols.TABLE_OWNER as OWNER, cols.TABLE_NAME, cols.COLUMN_NAME
from ALL_IND_COLUMNS cols
inner join all_indexes inds
	on inds.TABLE_OWNER = cols.TABLE_OWNER and inds.TABLE_NAME = cols.TABLE_NAME and inds.INDEX_NAME = inds.INDEX_NAME
where
	%v
    and inds.DROPPED = 'NO'
    and inds.STATUS = 'VALID'
    and inds.UNIQUENESS = 'UNIQUE'
	and EXISTS(select * from ALL_TABLES tabs where tabs.OWNER = cols.TABLE_OWNER and tabs.TABLE_NAME = cols.TABLE_NAME)`,
		defaultCondition)
	//nolint:descriptiveerrors
	return db.loadIndexes(sql, IndexTypeUnique)
}

type IndexRow struct {
	IndexName  string `db:"INDEX_NAME"`
	SchemaName string `db:"OWNER"`
	TableName  string `db:"TABLE_NAME"`
	ColumnName string `db:"COLUMN_NAME"`
}

func (db *Database) loadIndexes(sql string, indexType IndexType) error {
	return oracle_common.PDBQueryGlobal(db.config, db.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			rows, err := connection.QueryxContext(ctx, sql)
			if err != nil {
				return xerrors.Errorf("Can't get indexes from DB: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var row IndexRow
				if err := rows.StructScan(&row); err != nil {
					return xerrors.Errorf("Can't parse row from DB: %w", err)
				}

				tableID := oracle_common.NewTableID(row.SchemaName, row.TableName)
				table := db.OracleTableByID(tableID)
				if table == nil {
					return xerrors.Errorf("Table '%v' not in loaded schema", tableID.OracleSQLName())
				}

				column := table.OracleColumnByName(row.ColumnName)
				if column == nil {
					return xerrors.Errorf("Column '%v' not in loaded schema", row.ColumnName)
				}

				index := table.OracleIndexByName(row.IndexName)
				if index == nil {
					index = NewIndex(table, row.IndexName, indexType)
					table.addIndex(index)
				}
				index.addColumn(column)
			}

			if rows.Err() != nil {
				return xerrors.Errorf("Can't read row from DB: %w", rows.Err())
			}

			return nil
		})
}
