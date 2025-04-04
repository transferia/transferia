package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"go.ytsaurus.tech/library/go/core/log"
)

func timescaleDBSchemas() []string {
	return []string{
		"_timescaledb_debug",
		"_timescaledb_cache",
		"_timescaledb_catalog",
		"_timescaledb_functions",
		"_timescaledb_internal",
		"_timescaledb_config",
		"timescaledb_information",
		"timescaledb_experimental",
	}
}

func pgSystemSchemas() []string {
	return append([]string{"pg_catalog", "information_schema"}, timescaleDBSchemas()...)
}

func pgSystemTableNames() []string {
	return []string{"repl_mon", "pg_stat_statements"}
}

type SchemaExtractor struct {
	excludeViews      bool
	useFakePrimaryKey bool
	forbiddenSchemas  []string
	forbiddenTables   []string
	flavour           DBFlavour
	logger            log.Logger
}

func NewSchemaExtractor() *SchemaExtractor {
	return &SchemaExtractor{
		excludeViews:      false,
		useFakePrimaryKey: false,
		forbiddenSchemas:  pgSystemSchemas(),
		forbiddenTables:   pgSystemTableNames(),
		flavour:           NewPostgreSQLFlavour(),
		logger:            logger.Log,
	}
}

func (e *SchemaExtractor) WithExcludeViews(excludeViews bool) *SchemaExtractor {
	e.excludeViews = excludeViews
	return e
}

func (e *SchemaExtractor) WithUseFakePrimaryKey(useFakePrimaryKey bool) *SchemaExtractor {
	e.useFakePrimaryKey = useFakePrimaryKey
	return e
}

func (e *SchemaExtractor) WithForbiddenSchemas(forbiddenSchemas []string) *SchemaExtractor {
	e.forbiddenSchemas = forbiddenSchemas
	return e
}

func (e *SchemaExtractor) WithForbiddenTables(forbiddenTables []string) *SchemaExtractor {
	e.forbiddenTables = forbiddenTables
	return e
}

func (e *SchemaExtractor) WithFlavour(flavour DBFlavour) *SchemaExtractor {
	e.flavour = flavour
	return e
}

func (e *SchemaExtractor) WithLogger(logger log.Logger) *SchemaExtractor {
	e.logger = logger
	return e
}

// LoadSchema returns a settings-customized schema(s) of table(s) in PostgreSQL
func (e *SchemaExtractor) LoadSchema(ctx context.Context, conn *pgx.Conn, specificTable *abstract.TableID) (abstract.DBSchema, error) {
	tableColumns, err := e.tableToColumnsMapping(ctx, conn, specificTable)
	if err != nil {
		return nil, xerrors.Errorf("failed to list tables' columns: %w", err)
	}

	tablePKs, err := e.tableToPKColumnsMapping(ctx, conn, specificTable)
	if err != nil {
		return nil, xerrors.Errorf("failed to list tables' primary keys: %w", err)
	}

	replIdentFullTables := make(map[abstract.TableID]bool)
	for tID := range tableColumns {
		if _, ok := tablePKs[tID]; !ok {
			// query REPLICA IDENTITY FULL only when there is at least one table for which this can be useful
			if replIdentFullTables, err = e.replicaIdentityFullTables(ctx, conn, specificTable); err != nil {
				return nil, xerrors.Errorf("failed to list tables with REPLICA IDENTITY FULL: %w", err)
			}
			break
		}
	}

	result := make(abstract.DBSchema)
	for k, v := range tableColumns {
		ts := makeTableSchema(v, tablePKs[k])
		tsCanBeWholePK := replIdentFullTables[k] || e.useFakePrimaryKey
		if !ts.HasPrimaryKey() && tsCanBeWholePK {
			for i := range ts {
				ts[i].PrimaryKey = true
				ts[i].FakeKey = true
			}
		}
		result[k] = abstract.NewTableSchema(ts)
	}

	return result, nil
}

// listSchemaQuery returns a SQL query without placeholders when the given table is `nil`, or with two placeholders otherwise
func (e *SchemaExtractor) listSchemaQuery(specificTable *abstract.TableID) string {
	return e.flavour.ListSchemaQuery(e.excludeViews, specificTable != nil, e.forbiddenSchemas, e.forbiddenTables)
}

func specificTableLogField(specificTable *abstract.TableID) log.Field {
	if specificTable == nil {
		return log.Nil("table")
	} else {
		return log.String("table", specificTable.Fqtn())
	}
}

func unpackEnumValues(in interface{}) []string {
	if in != nil {
		pgGenericArray := in.(*GenericArray)
		arr, _ := pgGenericArray.ExtractValue(pgtype.NewConnInfo())
		var result []string
		for _, el := range arr.([]interface{}) {
			result = append(result, el.(string))
		}
		return result
	}
	return nil
}

func (e *SchemaExtractor) tableToColumnsMapping(ctx context.Context, conn *pgx.Conn, specificTable *abstract.TableID) (map[abstract.TableID]abstract.TableColumns, error) {
	query := e.listSchemaQuery(specificTable)
	e.logger.Debug("Retrieving schema from source using PostgreSQL schema extractor", specificTableLogField(specificTable), log.String("query", query))
	var rows pgx.Rows
	var err error
	if specificTable != nil {
		rows, err = conn.Query(ctx, query, specificTable.Namespace, specificTable.Name)
	} else {
		rows, err = conn.Query(ctx, query)
	}
	if err != nil {
		return nil, xerrors.Errorf("failed to execute schema retrieval SQL: %w", err)
	}
	defer rows.Close()

	result := make(map[abstract.TableID]abstract.TableColumns)

	for rows.Next() {
		var dummy int
		var tSchema, tName, cName, cDefault, dataType, dataTypeUnderlyingUnderDomain, expr, dtSchema string
		var allEnumValues interface{}
		var domainName *string
		var isNullable bool
		err := rows.Scan(
			&tSchema,
			&tName,
			&cName,
			&cDefault,
			&dataType,
			&dtSchema,
			&domainName,
			&dataTypeUnderlyingUnderDomain,
			&allEnumValues,
			&expr,
			&dummy,
			&isNullable,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan from schema retrieval query: %w", err)
		}
		if dataTypeUnderlyingUnderDomain == "USER-DEFINED" && !strings.Contains(dataType, dtSchema) && dtSchema != "public" {
			dataType = fmt.Sprintf("%s.%s", dtSchema, dataType)
		}
		originalType := dataType
		if domainName != nil {
			dataType = dataTypeUnderlyingUnderDomain
		}
		col := abstract.ColSchema{
			TableSchema:  tSchema,
			TableName:    tName,
			Path:         "",
			ColumnName:   cName,
			DataType:     string(PgTypeToYTType(dataType)),
			PrimaryKey:   false,
			FakeKey:      false,
			Required:     !isNullable,
			Expression:   expr,
			OriginalType: fmt.Sprintf("pg:%v", originalType),
			Properties:   nil,
		}
		allEnumValuesArr := unpackEnumValues(allEnumValues)
		if allEnumValuesArr != nil {
			col.AddProperty(EnumAllValues, allEnumValuesArr)
		}
		if IsPgTypeTimestampWithoutTimeZoneUnprefixed(originalType) {
			col.AddProperty(DatabaseTimeZone, conn.PgConn().ParameterStatus(TimeZoneParameterStatusKey))
		}
		if cDefault != "" && !strings.HasPrefix(cDefault, "nextval") { // nextval no need to replicate
			cDefault = strings.TrimSuffix(cDefault, "::"+dataType) // cut-off postgres specific type-cast
			col.AddProperty(changeitem.DefaultPropertyKey, cDefault)
		}
		result[col.TableID()] = append(result[col.TableID()], col)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to get next row from schema retrieval query: %w", err)
	}

	e.logger.Debug("Schema retrieved successfully")
	return result, nil
}

// listPKeysQuery returns a SQL query without placeholders when the given table is `nil`, or with two placeholders otherwise
func (e *SchemaExtractor) listPKeysQuery(specificTable *abstract.TableID) string {
	// See documentation on PostgreSQL service relations and views used in this query:
	// https://www.postgresql.org/docs/9.4/catalog-pg-class.html
	// https://www.postgresql.org/docs/9.4/catalog-pg-namespace.html
	// https://www.postgresql.org/docs/9.4/catalog-pg-index.html
	// https://www.postgresql.org/docs/9.4/catalog-pg-constraint.html
	// https://www.postgresql.org/docs/9.4/functions-info.html
	return fmt.Sprintf(`WITH
	table_names AS (
		SELECT
			c.oid AS cl,
			ns.nspname AS table_schema,
			c.relname::TEXT AS table_name
		FROM
			pg_class c
			INNER JOIN pg_namespace ns ON c.relnamespace = ns.oid
		WHERE %[1]s%[2]s
	),
	unique_indexes AS (
		SELECT DISTINCT ON (indrelid)
			indrelid,
			indkey
		FROM pg_index
		WHERE
			indpred IS NULL
			and indisunique
			and indisreplident
		ORDER BY indrelid, indnatts ASC, indexrelid
	),
	pkeys AS (
		SELECT
			conrelid,
			conkey
		FROM pg_constraint
		WHERE contype = 'p'
	),
	replica_identity AS (
		SELECT
			COALESCE(conrelid, indrelid) as relid,
			COALESCE(conkey, indkey) as key
		FROM unique_indexes
		FULL OUTER JOIN pkeys ON indrelid = conrelid
	),
	primary_keys_columns AS (
		SELECT
			UNNEST(replica_identity.key) AS col_num,
			table_names.table_name,
			table_names.table_schema,
			table_names.cl
		FROM replica_identity
		JOIN table_names ON replica_identity.relid = table_names.cl
	),
	ordered_primary_keys_columns AS (
		SELECT
			ROW_NUMBER() OVER () AS row_number,
			col_num,
			table_name,
			table_schema,
			cl
		FROM primary_keys_columns
	)
SELECT
	table_schema,
	table_name,
	attname
FROM
	pg_attribute
	JOIN ordered_primary_keys_columns ON (attrelid = cl AND col_num = attnum)
WHERE
	has_column_privilege(ordered_primary_keys_columns.cl, ordered_primary_keys_columns.col_num, 'SELECT')
ORDER BY row_number`,
		func() string {
			if specificTable != nil {
				return `ns.nspname = $1 AND c.relname = $2`
			}
			return fmt.Sprintf(`ns.nspname NOT IN (%[1]s) AND c.relname NOT IN (%[2]s)`, ListWithCommaSingleQuoted(e.forbiddenSchemas), ListWithCommaSingleQuoted(e.forbiddenTables))
		}(),
		func() string {
			if e.excludeViews {
				return " AND (" + e.flavour.PgClassRelsOnlyFilter() + ")"
			}
			return " AND (" + e.flavour.PgClassFilter() + ")"
		}(),
	)
}

func (e *SchemaExtractor) tableToPKColumnsMapping(ctx context.Context, conn *pgx.Conn, specificTable *abstract.TableID) (
	result map[abstract.TableID] /* column names */ []string,
	err error,
) {
	query := e.listPKeysQuery(specificTable)
	e.logger.Debug("Retrieving primary keys", log.String("query", query), specificTableLogField(specificTable))
	var rows pgx.Rows
	if specificTable != nil {
		rows, err = conn.Query(ctx, query, specificTable.Namespace, specificTable.Name)
	} else {
		rows, err = conn.Query(ctx, query)
	}
	if err != nil {
		return nil, xerrors.Errorf("failed to execute SQL to list primary keys: %w", err)
	}
	defer rows.Close()

	result = make(map[abstract.TableID][]string)
	for rows.Next() {
		var col abstract.ColSchema
		err = rows.Scan(&col.TableSchema, &col.TableName, &col.ColumnName)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan from primary keys list query: %w", err)
		}
		result[col.TableID()] = append(result[col.TableID()], col.ColumnName)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to get next row from primary keys list query: %w", err)
	}

	childParentMap, err := MakeChildParentMap(ctx, conn)
	if err != nil {
		return nil, xerrors.Errorf("failed while reading pg_inherits: %w", err)
	}

	for child, parent := range childParentMap {
		if len(result[child]) == 0 {
			result[child] = result[parent]
		}
	}

	return result, nil
}

// replicaIdentityFullListTablesQuery returns a SQL query without placeholders when the given table is `nil`, or with two placeholders otherwise
func (e *SchemaExtractor) replicaIdentityFullListTablesQuery(specificTable *abstract.TableID) string {
	// See documentation on PostgreSQL service relations and views used in this query:
	// https://www.postgresql.org/docs/9.4/catalog-pg-class.html
	// https://www.postgresql.org/docs/9.4/catalog-pg-namespace.html
	return fmt.Sprintf(`SELECT
	pgn.nspname AS nspname,
	pgc.relname AS relname
FROM
	pg_catalog.pg_class pgc
	LEFT JOIN pg_catalog.pg_namespace pgn ON (pgc.relnamespace = pgn.oid)
WHERE
	pgc.relreplident = 'f' AND pgc.relkind = 'r'
	AND %[1]s`,
		func() string {
			if specificTable != nil {
				return `nspname = $1 AND relname = $2`
			}
			return fmt.Sprintf(`nspname NOT IN (%[1]s) AND relname NOT IN (%[2]s)`, ListWithCommaSingleQuoted(e.forbiddenSchemas), ListWithCommaSingleQuoted(e.forbiddenTables))
		}(),
	)
}

func (e *SchemaExtractor) replicaIdentityFullTables(ctx context.Context, conn *pgx.Conn, specificTable *abstract.TableID) (map[abstract.TableID]bool, error) {
	query := e.replicaIdentityFullListTablesQuery(specificTable)
	e.logger.Info("Retrieving REPLICA IDENTITY FULL TABLEs", log.String("query", query), specificTableLogField(specificTable))
	var rows pgx.Rows
	var err error
	if specificTable != nil {
		rows, err = conn.Query(ctx, query, specificTable.Namespace, specificTable.Name)
	} else {
		rows, err = conn.Query(ctx, query)
	}
	if err != nil {
		return nil, xerrors.Errorf("failed to execute SQL: %w", err)
	}
	defer rows.Close()

	result := make(map[abstract.TableID]bool)

	for rows.Next() {
		var tableID abstract.TableID
		if err := rows.Scan(&tableID.Namespace, &tableID.Name); err != nil {
			return nil, xerrors.Errorf("failed to scan: %w", err)
		}
		result[tableID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to get next row: %w", err)
	}

	return result, nil
}

func (e *SchemaExtractor) listTablesQuery() string {
	return e.flavour.ListTablesQuery(e.excludeViews, e.forbiddenSchemas, e.forbiddenTables)
}

type tableIDWithInfo struct {
	ID   abstract.TableID
	Info abstract.TableInfo
}

type tableIDWithInfos []tableIDWithInfo

func (s tableIDWithInfos) String() string {
	result := make([]string, 0)
	for _, tID := range s {
		result = append(result, tID.ID.Fqtn())
	}
	return strings.Join(result, ", ")
}

// TablesList returns a list of basic information pieces about all tables in the given schema
func (e *SchemaExtractor) TablesList(ctx context.Context, conn *pgx.Conn) ([]tableIDWithInfo, time.Time, error) {
	var ts time.Time
	query := e.listTablesQuery()
	e.logger.Info("Retrieving a list of tables", log.String("query", query))
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, ts, xerrors.Errorf("unable to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(context.TODO(), query)
	if err != nil {
		return nil, ts, xerrors.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	result := make([]tableIDWithInfo, 0)
	for rows.Next() {
		var tID abstract.TableID
		var tInfo abstract.TableInfo
		var relKind string
		var etaRow int64 // doc: "If the table has never yet been vacuumed or analyzed, reltuples contains -1 indicating that the row count is unknown."
		if err := rows.Scan(&tID.Namespace, &tID.Name, &relKind, &etaRow); err != nil {
			return nil, ts, xerrors.Errorf("failed to scan from list tables query: %w", err)
		}
		if relKind == "v" {
			tInfo.IsView = true
		}
		if etaRow < 0 { // for parent tables etaRow will be sum of reltuples values its descendants thus etaRow could has -N value
			etaRow = 0
		}
		tInfo.EtaRow = uint64(etaRow)
		result = append(result, tableIDWithInfo{
			ID:   tID,
			Info: tInfo,
		})
	}
	if rows.Err() != nil {
		return nil, ts, xerrors.Errorf("failed to get next row for list tables query: %w", err)
	}
	if err := tx.QueryRow(ctx, `select now()`).Scan(&ts); err != nil {
		return result, ts, nil
	}

	return result, ts, nil
}

func (e *SchemaExtractor) FindDependentViews(ctx context.Context, conn *pgx.Conn, tables abstract.TableMap) (map[abstract.TableID][]abstract.TableID, error) {
	dependentViewsQuery := e.getDependentViewsQuery(tables)
	e.logger.Info(
		"Retrieving dependent views",
		log.String("query", dependentViewsQuery),
	)

	var rows pgx.Rows
	var err error
	startTime := time.Now()
	rows, err = conn.Query(ctx, dependentViewsQuery)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute dependent views extracting query: %w", err)
	}
	queryDuration := time.Since(startTime)
	defer rows.Close()
	views := make(map[abstract.TableID][]abstract.TableID)
	for rows.Next() {
		var tableID abstract.TableID
		var viewID abstract.TableID
		if err := rows.Scan(&tableID.Namespace, &tableID.Name, &viewID.Namespace, &viewID.Name); err != nil {
			return nil, xerrors.Errorf("failed to scan from list dependent views query: %w", err)
		}
		if _, ok := tables[tableID]; ok {
			if _, viewsOk := views[tableID]; !viewsOk {
				views[tableID] = make([]abstract.TableID, 0)
			}
			views[tableID] = append(views[tableID], viewID)
		}
	}
	if rows.Err() != nil {
		return nil, xerrors.Errorf("failed to get next row for list dependent views query: %w", err)
	}
	e.logger.Infof("dependent views retrieving elapsed total: %v, query: %v", time.Since(startTime), queryDuration)
	return views, nil
}

// TODO: implement Set for GenericArray https://github.com/transferia/transferia/arc/trunk/arcadia/transfer_manager/go/pkg/dataagent/pg/generic_array.go?rev=r9238739#L132
func (e *SchemaExtractor) getDependentViewsQuery(tables abstract.TableMap) string {
	schemas, names := prepareSchemasAndNamesParams(tables)
	return fmt.Sprintf(`
                SELECT distinct
                        source_ns.nspname as source_schema,
                        source_table.relname as source_table,
                        dependent_ns.nspname as dependent_schema,
                        dependent_view.relname as dependent_view
                FROM pg_depend
                JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
                JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
                JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
                JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
                        AND pg_depend.refobjsubid = pg_attribute.attnum
                JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
                JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
                WHERE
                        source_ns.nspname IN (%s)
                        AND source_table.relname IN (%s);`,
		schemas,
		names,
	)
}

func prepareSchemasAndNamesParams(tables abstract.TableMap) (schemas, names string) {
	schemasSet := make(map[string]bool)
	namesSet := make(map[string]bool)

	for id := range tables {
		schemasSet[id.Namespace] = true
		namesSet[id.Name] = true
	}

	schemasList := make([]string, len(schemasSet))
	i := 0
	for schema := range schemasSet {
		schemasList[i] = schema
		i++
	}
	schemas = ListWithCommaSingleQuoted(schemasList)

	namesList := make([]string, len(namesSet))
	j := 0
	for name := range namesSet {
		namesList[j] = name
		j++
	}
	names = ListWithCommaSingleQuoted(namesList)
	return schemas, names
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
		if result[i].PrimaryKey {
			result[i].Expression = ""
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].PrimaryKey && !result[j].PrimaryKey })
	sort.SliceStable(result[:len(pkeys)], func(i, j int) bool { return pkeyMap[result[i].ColumnName] < pkeyMap[result[j].ColumnName] })

	return result
}
