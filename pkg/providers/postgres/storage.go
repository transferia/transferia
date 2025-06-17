//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dblog/tablequery"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type SortOrder string

type DuplicatesPolicy string

const (
	SortAsc  SortOrder = "asc"
	SortDesc SortOrder = "desc"

	All DuplicatesPolicy = "all"

	PartitionsFilterPrefix = "__dt_resolved_hard:"
)

type SnapshotKey string

func (c SnapshotKey) String() string {
	return "snapshot_" + string(c)
}

type SnapshotState struct {
	SnapshotLSN string
	SnapshotTS  int64
}

type Storage struct {
	Config              *PgStorageParams
	Conn                *pgxpool.Pool
	version             PgVersion
	snapshotConnection  *pgxpool.Conn
	snapshotTransaction pgx.Tx
	IsHomo              bool
	once                sync.Once
	metrics             *stats.SourceStats
	ForbiddenSchemas    []string
	ForbiddenTables     []string
	Flavour             DBFlavour
	typeNameToOID       TypeNameToOIDMap
	ShardedStateLSN     string
	ShardedStateTS      time.Time
	loadDescending      bool
	sExTime             time.Time
}

func (s *Storage) SetLoadDescending(loadDescending bool) {
	s.loadDescending = loadDescending
}

func (s *Storage) SnapshotLSN() string {
	return s.ShardedStateLSN
}

func (s *Storage) WorkersSnapshotState() *SnapshotState {
	return &SnapshotState{
		SnapshotLSN: s.ShardedStateLSN,
		SnapshotTS:  s.ShardedStateTS.UnixMilli(),
	}
}

func (s *Storage) ShardingContext() ([]byte, error) {
	logger.Log.Infof("Forming sharding state: LSN %v: snapshot_time: %v", s.ShardedStateLSN, s.ShardedStateTS)
	jsonctx, err := json.Marshal(s.WorkersSnapshotState())
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal pg config: %w", err)
	}
	return jsonctx, nil
}

func (s *Storage) SetShardingContext(shardedState []byte) error {
	logger.Log.Info("Setting sharding state to worker")
	res := new(SnapshotState)
	if err := json.Unmarshal(shardedState, res); err != nil {
		return xerrors.Errorf("unable to restore sharding state back to proto: %w", err)
	}
	s.ShardedStateLSN = res.SnapshotLSN
	s.ShardedStateTS = time.UnixMilli(res.SnapshotTS)

	if s.ShardedStateLSN == "" {
		return abstract.NewFatalError(xerrors.New("pg config is missing from sharded state"))
	}
	logger.Log.Infof("Sharded state: LSN %v: snapshot_time: %v", s.ShardedStateLSN, s.ShardedStateTS)
	return nil
}

func (s *Storage) Close() {
	s.once.Do(func() {
		if s.snapshotTransaction != nil {
			_ = s.snapshotTransaction.Rollback(context.TODO())
			s.snapshotTransaction = nil
		}
		if s.snapshotConnection != nil {
			s.snapshotConnection.Release()
			s.snapshotConnection = nil
		}
		s.Conn.Close()
	})
}

func isFilterEmpty(filter abstract.WhereStatement) bool {
	return len(filter) == 0
}

func WhereClause(filter abstract.WhereStatement) string {
	if partitionFilterTableName(filter) != "" {
		partitionFilter := partitionFilterTableFilter(filter)
		if partitionFilter != "" {
			return partitionFilter
		} else {
			return "1 = 1"
		}
	}
	if !isFilterEmpty(filter) {
		return string(filter)
	}

	return "1 = 1"
}

func partitionFilterTableFilter(filter abstract.WhereStatement) string {
	index := strings.Index(string(filter), "|")
	return string(filter)[index+1:]
}

func partitionFilterTableName(filter abstract.WhereStatement) string {
	if strings.HasPrefix(string(filter), PartitionsFilterPrefix) {
		result := strings.ReplaceAll(string(filter), PartitionsFilterPrefix, "")
		index := strings.Index(result, "|")
		return result[0:index]
	}
	return ""
}

func exactCountQuery(t *abstract.TableDescription) string {
	return fmt.Sprintf("select count(1) from %s where %s", TableName(t), WhereClause(t.Filter))
}

func TableName(t *abstract.TableDescription) string {
	if partitionFilterTableName(t.Filter) != "" {
		return partitionFilterTableName(t.Filter)
	}
	return t.Fqtn()
}

func ReadQuery(t *abstract.TableDescription, columns string) string {
	return fmt.Sprintf("select %s from %s where %s", columns, TableName(t), WhereClause(t.Filter))
}

func checkAccessibilityQuery(t *abstract.TableDescription) string {
	return fmt.Sprintf("select 1 from %s limit 1;", t.Fqtn())
}

func LockQuery(t abstract.TableID, mode PGTableLockMode) string {
	return fmt.Sprintf("LOCK TABLE %s IN %s MODE", t.Fqtn(), string(mode))
}

func typeMapQuery() string {
	return `
		select
			nspname	         as namespace,
			typname          as name,
			MAX(pg_type.oid) as oid
		from
			pg_type
		join pg_namespace on pg_type.typnamespace = pg_namespace.oid
		where
			nspname not in (
				'pg_catalog',
				'information_schema',
				'pg_toast'
			)
		group by
			nspname,
			typname
		;
	`
}

type PGTableLockMode string

const (
	AccessShareLockMode          PGTableLockMode = "ACCESS SHARE"
	RowShareLockMode             PGTableLockMode = "ROW SHARE"
	RowExclusiveLockMode         PGTableLockMode = "ROW EXCLUSIVE"
	ShareUpdateExclusiveLockMode PGTableLockMode = "SHARE UPDATE EXCLUSIVE"
	ShareLockMode                PGTableLockMode = "SHARE"
	ShareRowExclusiveLockMode    PGTableLockMode = "SHARE ROW EXCLUSIVE"
	ExclusiveLockMode            PGTableLockMode = "EXCLUSIVE"
	AccessExclusiveLockMode      PGTableLockMode = "ACCESS EXCLUSIVE"
)

func makeFromStatement(t *abstract.TableDescription, excludeDescendants bool) string {
	if excludeDescendants {
		return fmt.Sprintf("only %v", TableName(t))
	}
	return TableName(t)
}

func (s *Storage) OrderedRead(
	t *abstract.TableDescription,
	schemas []abstract.ColSchema,
	sortOrder SortOrder,
	filter abstract.WhereStatement,
	duplicatesPolicy DuplicatesPolicy,
	excludeDescendants bool,
) string {
	fromStatement := makeFromStatement(t, excludeDescendants)

	keyCols := make([]string, 0)
	cols := make([]string, 0)

	for _, col := range schemas {
		if s.Config.IgnoreUserTypes {
			if IsUserDefinedType(&col) {
				continue
			}
		}
		if col.PrimaryKey && col.ColumnName != "" {
			keyCols = append(keyCols, fmt.Sprintf("\"%v\"", col.ColumnName))
		}
		if col.OriginalType == "pg:hstore" && !s.IsHomo {
			cols = append(cols, fmt.Sprintf("\"%v\"::json", col.ColumnName))
		} else {
			cols = append(cols, fmt.Sprintf("\"%v\"", col.ColumnName))
		}
	}

	if len(keyCols) == 0 {
		return ReadQuery(t, "*")
	}

	q := fmt.Sprintf(
		"select %v %v from %s where (%s) and (%s)",
		duplicatesPolicy,
		strings.Join(cols, ","),
		fromStatement,
		WhereClause(t.Filter),
		WhereClause(filter),
	)
	if t.Offset != 0 {
		q += fmt.Sprintf(
			" order by %s %s  OFFSET %v",
			strings.Join(keyCols, ","),
			sortOrder,
			t.Offset,
		)
	}
	return q
}

func (s *Storage) Ping() error {
	return nil
}

// TableList in PostgreSQL returns a table map with schema
func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	ctx := context.TODO()

	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection from pool: %w", err)
	}
	defer conn.Release()

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, xerrors.Errorf("failed to start a transaction in storage: %w", err)
	}
	rollbacks.Add(func() {
		if err := tx.Rollback(ctx); err != nil {
			logger.Log.Warn("Failed to ROLLBACK table list transaction", log.Error(err))
		}
	})

	warnTooLongExec := util.DelayFunc(
		func() {
			logger.Log.Warn("Schema retrieval takes longer than usual. Check the list of tables included in the transfer and the load of source database.")
		},
		5*time.Minute,
	)
	defer warnTooLongExec.Cancel()

	sEx := NewSchemaExtractor().
		WithUseFakePrimaryKey(s.Config.UseFakePrimaryKey).
		WithExcludeViews(s.IsHomo).
		WithForbiddenSchemas(s.ForbiddenSchemas).
		WithForbiddenTables(s.ForbiddenTables).
		WithFlavour(s.Flavour)

	tablesListUnfiltered, ts, err := sEx.TablesList(ctx, tx.Conn())
	if err != nil {
		return nil, xerrors.Errorf("failed to load a list of tables in schema *: %w", err)
	}
	logger.Log.Info("Extracted tables (unfiltered)", log.String("tables", tableIDWithInfos(tablesListUnfiltered).String()))
	s.sExTime = ts

	result := make(abstract.TableMap)

	if s.Config.TableFilter != nil || filter != nil {
		// filter tables
		tablesListFiltered := make([]tableIDWithInfo, 0)
		for _, tIDWithInfo := range tablesListUnfiltered {
			if s.Config.TableFilter != nil && !s.Config.TableFilter.Include(tIDWithInfo.ID) {
				continue
			}
			if filter != nil && !filter.Include(tIDWithInfo.ID) {
				continue
			}
			tablesListFiltered = append(tablesListFiltered, tIDWithInfo)
		}
		logger.Log.Info("Extracted tables (filtered)", log.String("tables", tableIDWithInfos(tablesListFiltered).String()))
		for _, table := range tablesListFiltered {
			if err := loadIntoTableMapForTable(ctx, table, sEx, tx.Conn(), result); err != nil {
				return nil, xerrors.Errorf("failed to load schema for table %s: %w", table.ID.Fqtn(), err)
			}
		}
	} else {
		if err := loadIntoTableMap(ctx, tablesListUnfiltered, sEx, tx.Conn(), result); err != nil {
			return nil, xerrors.Errorf("failed to load schema: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, xerrors.Errorf("failed to COMMIT table list transaction: %w", err)
	}
	rollbacks.Cancel()

	return result, nil
}

func loadIntoTableMap(ctx context.Context, tables []tableIDWithInfo, extractor *SchemaExtractor, conn *pgx.Conn, result abstract.TableMap) error {
	schemas, err := extractor.LoadSchema(ctx, conn, nil)
	if err != nil {
		return xerrors.Errorf("failed to load schema using a query without filtering: %w", err)
	}
	for _, table := range tables {
		schema, ok := schemas[table.ID]
		if !ok {
			return xerrors.Errorf("failed to load schema for table %s using a query without filtering: schema is missing in query results", table.ID.Fqtn())
		}
		table.Info.Schema = schema
		result[table.ID] = table.Info
	}
	return nil
}

func loadIntoTableMapForTable(ctx context.Context, table tableIDWithInfo, extractor *SchemaExtractor, conn *pgx.Conn, result abstract.TableMap) error {
	tSchema, err := extractor.LoadSchema(ctx, conn, &table.ID)
	if err != nil {
		return xerrors.Errorf("failed to load schema for table %s using a query with filtering: %w", table.ID.Fqtn(), err)
	}
	table.Info.Schema = tSchema[table.ID]
	result[table.ID] = table.Info
	return nil
}

func (s *Storage) GetInheritedTables(ctx context.Context) (map[abstract.TableID]abstract.TableID, error) {
	tablesParents, err := MakeChildParentMap(ctx, s.Conn)
	if err != nil {
		logger.Log.Error("failed query for extraction pg inherits", log.Error(err))
		return nil, xerrors.Errorf("failed query for extraction pg inherits: %w", err)
	}
	if s.Config.TableFilter == nil {
		return tablesParents, nil
	}
	for childID := range tablesParents {
		if !s.Config.TableFilter.Include(childID) {
			delete(tablesParents, childID)
		}
	}

	return tablesParents, nil
}

func (s *Storage) ExactTableDescriptionRowsCount(ctx context.Context, table abstract.TableDescription, timeout time.Duration) (uint64, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	query := exactCountQuery(&table)
	var count uint64
	if err := s.Conn.QueryRow(ctxWithTimeout, query).Scan(&count); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			logger.Log.Warn("Calculating table rows count took too long - we recomment create index on cursor column", log.String("table", table.String()))
			return 0, nil
		}
		return 0, xerrors.Errorf("failed to count rows in table %s: %w", table.String(), err)
	}
	return count, nil
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	tinfo, err := s.getLoadTableMode(context.TODO(), abstract.TableDescription{
		Name:   table.Name,
		Schema: table.Namespace,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	})
	if err != nil {
		return 0, xerrors.Errorf("unable to build table info: %w", err)
	}
	if tinfo.tableInfo.HasSubclass || tinfo.tableInfo.IsInherited {
		var etaRow int64
		if err := s.Conn.QueryRow(context.TODO(), `
SELECT COALESCE(sum(reltuples), 0)
FROM pg_inherits inh
JOIN pg_class ON inh.inhrelid = pg_class.oid
WHERE
  inhparent :: regclass = $1 :: regclass;
`, table.Fqtn()).Scan(&etaRow); err != nil {
			return 0, xerrors.Errorf("unable to select sub-tables size: %w", err)
		}
		return uint64(etaRow), nil
	}
	var etaRow int64
	err = s.Conn.QueryRow(context.TODO(), fmt.Sprintf(`
		SELECT
			reltuples as eta_row
		FROM pg_class
		WHERE oid = ('%v')::regclass
		;
	`, table.Fqtn())).Scan(&etaRow)
	if err != nil {
		return 0, xerrors.Errorf("unable to estimate table rows for table %s.%s: %w", table.Namespace, table.Name, err)
	}
	if etaRow == -1 {
		etaRow = 0
	}
	return uint64(etaRow), nil
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	tx, err := s.Conn.BeginTx(context.TODO(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, xerrors.Errorf("unable to begin transaction to get exact table rows for table %s: %w", table.Fqtn(), err)
	}
	defer tx.Commit(context.TODO()) //nolint

	exactCountQueryStr := exactCountQuery(&abstract.TableDescription{Schema: table.Namespace, Name: table.Name, Filter: "", EtaRow: 0, Offset: 0})
	var totalCount uint64
	if err := tx.QueryRow(context.TODO(), exactCountQueryStr).Scan(&totalCount); err != nil {
		return 0, xerrors.Errorf("unable to count table rows: %w", err)
	}
	return totalCount, nil
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	exists := false
	err := s.Conn.QueryRow(context.Background(), fmt.Sprintf(`
		SELECT EXISTS (
		   SELECT FROM information_schema.tables
		   WHERE  table_schema = '%s'
		   AND    table_name   = '%s'
		   );
	`, table.Namespace, table.Name)).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// RowCount queries the storage and returns the exact number of rows in the given table
func RowCount(ctx context.Context, conn *pgx.Conn, table *abstract.TableDescription) (uint64, error) {
	query := exactCountQuery(table)
	var count uint64
	if err := conn.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, xerrors.Errorf("failed to count rows in table %s: %w", table.String(), err)
	}
	logger.Log.Info("The exact number of rows in source table has been calculated", log.String("table", table.String()), log.String("query", query), log.UInt64("rows", count))
	return count, nil
}

func MakeSetSQL(key string, value string) string {
	return strings.Join([]string{"SET", key, "=", value}, " ")
}

type loadTableMode struct {
	table          abstract.TableDescription
	tableInfo      tableInformationSchema
	isHomo         bool
	loadDescending bool // show whether we should load all child tables with parent table ID
}

func (m *loadTableMode) ExcludeDescendants() (exclude bool, reason string) {
	if m.tableInfo.HasSubclass && !m.loadDescending {
		exclude = true
		reason = fmt.Sprintf("Table %v is parent, use ONLY statement for selecting from it", m.table.Fqtn())
	}
	return exclude, reason
}

func (m *loadTableMode) SkipLoading() (skip bool, reason string) {
	if m.tableInfo.IsView && m.isHomo {
		skip = true
		reason = fmt.Sprintf("Table %s is a view, skipping it", m.table.Fqtn())
	} else if m.tableInfo.IsPartitioned && !m.loadDescending {
		skip = true
		reason = fmt.Sprintf("Table %v is partitioned, skipping it because it is empty and all data is in partitions", m.table.Fqtn())
	} else if m.tableInfo.IsInherited && m.loadDescending {
		skip = true
		reason = fmt.Sprintf("Table %v is child, but we load all in all-descending mode, skipping it because we may duplicate data within descendings", m.table.Fqtn())
	}
	return skip, reason
}

func (s *Storage) discoverTableLoadMode(ctx context.Context, conn pgxtype.Querier, table abstract.TableDescription) (*loadTableMode, error) {
	tableInfo, err := newTableInformationSchema(ctx, conn, table)
	if err != nil {
		return nil, xerrors.Errorf("failed to discover table information: %w", err)
	}
	return &loadTableMode{
		table:          table,
		tableInfo:      *tableInfo,
		isHomo:         s.IsHomo,
		loadDescending: s.loadDescending,
	}, nil
}

func readQueryParams(useBinarySerialization bool, nCols int) (params []interface{}) {
	if !useBinarySerialization {
		return nil
	}

	// In homogeneous pg->pg transfers we should get all data in binary
	// format, since PG sink uses COPY FROM, and the implementation of COPY
	// FROM in the pgx driver requires all types to be binary serializable.
	// Not all pgx types are binary serializable; e.g. pgtype.GenericText is
	// text-only.
	//
	// By specifying BinaryFormatCode for each column in the query we request
	// PostgreSQL to send us binary representations of the result values. For
	// well-known types it does not affect pg->pg copy in any way: pgx can
	// deserialize text values and then serialize them as binary when doing
	// COPY FROM. However, the unknown types (created by PostgreSQL extensions,
	// like Postgis) are handled by the catch-all types pgtype.GenericText and
	// pgtype.GenericBianary, which are text-only and binary-only,
	// respectively. Since for that types we cannot make text->binary
	// conversion, we want to get all the values from the PostgreSQL in binary
	// format in the first place.
	var formats pgx.QueryResultFormats
	for i := 0; i < nCols; i++ {
		formats = append(formats, pgx.BinaryFormatCode)
	}
	params = append(params, formats)

	// We alse have to disable simple protocol, otherwise the query result
	// formats are ignored by pgx.
	params = append(params, pgx.QuerySimpleProtocol(false))

	return params
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection from the pool: %w", err)
	}
	defer conn.Release()

	tx, txRollbacks, err := BeginTxWithSnapshot(ctx, conn.Conn(), repeatableReadReadOnlyTxOptions, s.ShardedStateLSN, logger.Log)
	if err != nil {
		return nil, xerrors.Errorf("failed to start a transaction: %w", err)
	}
	defer txRollbacks.Do()

	schema, err := s.LoadSchemaForTable(ctx, tx.Conn(), abstract.TableDescription{
		Name:   table.Name,
		Schema: table.Namespace,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get schema for table %s: %w", table.Fqtn(), err)
	}

	return schema, nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return xerrors.Errorf("failed to acquire a connection from the pool: %w", err)
	}
	defer conn.Release()

	loadMode, err := s.discoverTableLoadMode(ctx, conn, table)
	if err != nil {
		return xerrors.Errorf("failed to discover table %v loading mode: %w", table.Fqtn(), err)
	}

	if skip, reason := loadMode.SkipLoading(); skip {
		logger.Log.Infof("Skip load table %v: %v", table.Fqtn(), reason)
		return nil
	}

	excludeDescendants, reason := loadMode.ExcludeDescendants()
	if excludeDescendants {
		logger.Log.Infof("Use ONLY statement for selecting from table %v: %v", table.Fqtn(), reason)
	}

	if _, err := conn.Exec(ctx, MakeSetSQL("statement_timeout", "0")); err != nil {
		return xerrors.Errorf("failed to SET statement_timeout: %w", err)
	}

	tx, txRollbacks, err := BeginTxWithSnapshot(ctx, conn.Conn(), repeatableReadReadOnlyTxOptions, s.ShardedStateLSN, logger.Log)
	if err != nil {
		return xerrors.Errorf("failed to start a transaction: %w", err)
	}
	defer txRollbacks.Do()

	currentTS := s.ShardedStateTS
	logger.Log.Infof("Commit time from snapshot is: %v", currentTS)
	if currentTS.IsZero() {
		currentTS, err = CurrentTxStartTime(ctx, tx.Conn())
		logger.Log.Infof("Using start read time as commit time for snapshot: %v", currentTS)
		if err != nil {
			return xerrors.Errorf("failed to extract current transaction start time from source: %w", err)
		}
	}

	schema, err := s.LoadSchemaForTable(ctx, tx.Conn(), table)
	if err != nil {
		return xerrors.Errorf("failed to get schema for table %s: %w", table.Fqtn(), err)
	}

	readQuery := s.OrderedRead(&table, schema.Columns(), SortAsc, abstract.NoFilter, All, excludeDescendants)
	logger.Log.Info("SELECT query at source formed", log.String("table", table.String()), log.String("query", readQuery))

	if err := s.loadTable(ctx, table, pusher, readQuery, tx.Conn(), loadMode, schema, currentTS); err != nil {
		return xerrors.Errorf("unable to load table : %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return xerrors.Errorf("failed to commit transaction: %w", err)
	}
	txRollbacks.Cancel()
	logger.Log.Info("successfully committed transaction at source", log.String("fqtn", table.Fqtn()))

	return nil
}

func (s *Storage) LoadSchema() (abstract.DBSchema, error) {
	ctx := context.TODO()
	if conn, err := s.Conn.Acquire(ctx); err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection: %w", err)
	} else {
		defer conn.Release()
		return NewSchemaExtractor().
			WithUseFakePrimaryKey(s.Config.UseFakePrimaryKey).
			WithExcludeViews(s.IsHomo).
			WithForbiddenSchemas(s.ForbiddenSchemas).
			WithForbiddenTables(s.ForbiddenTables).
			WithFlavour(s.Flavour).
			LoadSchema(ctx, conn.Conn(), nil)
	}
}

func (s *Storage) LoadSchemaForTable(ctx context.Context, conn *pgx.Conn, table abstract.TableDescription) (*abstract.TableSchema, error) {
	tableID := table.ID()
	schemas, err := NewSchemaExtractor().
		WithUseFakePrimaryKey(s.Config.UseFakePrimaryKey).
		WithExcludeViews(s.IsHomo).
		WithForbiddenSchemas(s.ForbiddenSchemas).
		WithForbiddenTables(s.ForbiddenTables).
		WithFlavour(s.Flavour).
		LoadSchema(ctx, conn, &tableID)
	if err != nil {
		return nil, xerrors.Errorf("failed to query schema from the source database: %w", err)
	}
	if schema, ok := schemas[table.ID()]; !ok {
		return nil, xerrors.Errorf("a schema for table %s is not available in the list of schemas obtained from the source database", table.Fqtn())
	} else {
		return schema, nil
	}
}

func (s *Storage) loadSample(
	tx pgx.Tx,
	query string,
	table abstract.TableDescription,
	tableSchema *abstract.TableSchema,
	startTime time.Time,
	pusher abstract.Pusher,
) error {
	ctx := context.Background()

	queryParams := readQueryParams(s.Config.UseBinarySerialization, len(tableSchema.Columns()))
	rows, err := tx.Query(ctx, query, queryParams...)
	if err != nil {
		return xerrors.Errorf("failed to execute SELECT: %w", err)
	}
	defer rows.Close()

	ciFetcher := NewChangeItemsFetcher(rows, tx.Conn(), abstract.ChangeItem{
		ID:           uint32(0),
		LSN:          0,
		CommitTime:   uint64(startTime.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       table.Schema,
		Table:        table.Name,
		PartID:       table.PartID(),
		ColumnNames:  tableSchema.Columns().ColumnNames(),
		ColumnValues: nil,
		TableSchema:  tableSchema,
		OldKeys:      abstract.EmptyOldKeys(),
		TxID:         "",
		Query:        "",
		Size:         abstract.EmptyEventSize(),
	}, s.metrics).WithUnmarshallerData(MakeUnmarshallerData(s.IsHomo, tx.Conn())).WithLimitCount(2000)

	logger.Log.Info("extracting data from the source table...", log.String("fqtn", table.Fqtn()))
	for ciFetcher.MaybeHasMore() {
		items, err := ciFetcher.Fetch()
		if err != nil {
			return xerrors.Errorf("failed to extract data from the table %s: %w", table.Fqtn(), err)
		}
		if len(items) > 0 {
			if err := pusher(items); err != nil {
				return xerrors.Errorf("failed to push %d ChangeItems. Error: %w", len(items), err)
			}
		}
	}
	logger.Log.Info("successfully extracted data from the source table", log.String("fqtn", table.Fqtn()))

	return nil
}

func (s *Storage) LoadRandomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	startTime := time.Now()

	ctx := context.Background()

	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return xerrors.Errorf("unable to acquire connection: %w", err)
	}
	defer conn.Release()

	loadMode, err := s.discoverTableLoadMode(ctx, conn, table)
	if err != nil {
		return xerrors.Errorf("failed to discover table %v loading mode: %w", table.Fqtn(), err)
	}

	if skip, reason := loadMode.SkipLoading(); skip {
		logger.Log.Infof("Skip load table %v: %v", table.Fqtn(), reason)
		return nil
	}

	excludeDescendants, reason := loadMode.ExcludeDescendants()
	if excludeDescendants {
		logger.Log.Infof("Use ONLY statement for selecting from table %v: %v", table.Fqtn(), reason)
	}

	tx, err := conn.BeginTx(ctx, repeatableReadReadOnlyTxOptions)
	if err != nil {
		return xerrors.Errorf("unable to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Commit(ctx)
	}()

	schema, err := NewSchemaExtractor().
		WithUseFakePrimaryKey(s.Config.UseFakePrimaryKey).
		WithExcludeViews(s.IsHomo).
		WithForbiddenSchemas(s.ForbiddenSchemas).
		WithForbiddenTables(s.ForbiddenTables).
		WithFlavour(s.Flavour).
		LoadSchema(ctx, tx.Conn(), nil)
	if err != nil {
		return xerrors.Errorf("unable to load schema: %w", err)
	}

	rawSchema := schema[table.ID()]

	totalQ := s.OrderedRead(&table, rawSchema.Columns(), SortAsc, "random()<=0.05", All, excludeDescendants) + " limit 2000"
	logger.Log.Infof("LOAD SAMPLE QUERY: %v", totalQ)

	if err := s.loadSample(tx, totalQ, table, rawSchema, startTime, pusher); err != nil {
		return xerrors.Errorf("unable to load sample: %w", err)
	}

	return nil
}

func (s *Storage) LoadSampleBySet(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
	startTime := time.Now()

	conn, err := s.Conn.Acquire(context.TODO())
	if err != nil {
		return xerrors.Errorf("unable to acquire connection: %w", err)
	}
	defer conn.Release()

	ctx := context.Background()
	tx, err := conn.BeginTx(ctx, repeatableReadReadOnlyTxOptions)
	if err != nil {
		return xerrors.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Commit(context.TODO()) //nolint

	loadMode, err := s.discoverTableLoadMode(ctx, conn, table)
	if err != nil {
		return xerrors.Errorf("failed to discover table %v loading mode: %w", table.Fqtn(), err)
	}

	if skip, reason := loadMode.SkipLoading(); skip {
		logger.Log.Infof("Skip load table %v: %v", table.Fqtn(), reason)
		return nil
	}

	excludeDescendants, reason := loadMode.ExcludeDescendants()
	if excludeDescendants {
		logger.Log.Infof("Use ONLY statement for selecting from table %v: %v", table.Fqtn(), reason)
	}

	schema, err := NewSchemaExtractor().
		WithUseFakePrimaryKey(s.Config.UseFakePrimaryKey).
		WithExcludeViews(s.IsHomo).
		WithForbiddenSchemas(s.ForbiddenSchemas).
		WithForbiddenTables(s.ForbiddenTables).
		WithFlavour(s.Flavour).
		LoadSchema(ctx, tx.Conn(), nil)
	if err != nil {
		return xerrors.Errorf("unable to load schema: %w", err)
	}

	// TODO carefully on memory !
	var conditions []string
	for _, v := range keySet {
		var pkConditions []string
		for name, val := range v {
			// string values won't work,
			// we need to represent from sink
			var columnType abstract.ColSchema
			for _, schemaElem := range schema[table.ID()].Columns() {
				if schemaElem.ColumnName == name {
					columnType = schemaElem
					break
				}
			}
			repr, err := Represent(val, columnType)
			if err != nil {
				return xerrors.Errorf("unable to represent value: %w", err)
			}
			pkConditions = append(pkConditions, fmt.Sprintf("\"%v\"=%v", name, repr))
		}
		conditions = append(conditions, "("+strings.Join(pkConditions, " and ")+")")
	}
	var totalCondition string
	if len(conditions) > 0 {
		totalCondition = strings.Join(conditions, " or ")
	} else {
		totalCondition = "false"
	}

	rawSchema := schema[table.ID()]
	totalQ := s.OrderedRead(&table, rawSchema.Columns(), SortAsc, abstract.WhereStatement(totalCondition), All, excludeDescendants)

	if err := s.loadSample(tx, totalQ, table, rawSchema, startTime, pusher); err != nil {
		return xerrors.Errorf("unable to load sample: %w", err)
	}

	return nil
}

func (s *Storage) LoadTopBottomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	startTime := time.Now()
	ctx := context.Background()

	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return xerrors.Errorf("Failed to acquire a connection: %w", err)
	}
	defer conn.Release()

	loadMode, err := s.discoverTableLoadMode(ctx, conn, table)
	if err != nil {
		return xerrors.Errorf("failed to discover table %v loading mode: %w", table.Fqtn(), err)
	}

	if skip, reason := loadMode.SkipLoading(); skip {
		logger.Log.Infof("Skip load table %v: %v", table.Fqtn(), reason)
		return nil
	}

	excludeDescendants, reason := loadMode.ExcludeDescendants()
	if excludeDescendants {
		logger.Log.Infof("Use ONLY statement for selecting from table %v: %v", table.Fqtn(), reason)
	}

	tx, err := conn.BeginTx(ctx, repeatableReadReadOnlyTxOptions)
	if err != nil {
		return xerrors.Errorf("Failed to begin a transaction: %w", err)
	}
	defer func() {
		_ = tx.Commit(ctx)
	}()

	schema, err := NewSchemaExtractor().
		WithUseFakePrimaryKey(s.Config.UseFakePrimaryKey).
		WithExcludeViews(s.IsHomo).
		WithForbiddenSchemas(s.ForbiddenSchemas).
		WithForbiddenTables(s.ForbiddenTables).
		WithFlavour(s.Flavour).
		LoadSchema(ctx, tx.Conn(), nil)
	if err != nil {
		return xerrors.Errorf("Failed to load schema: %w", err)
	}

	rawSchema := schema[table.ID()]

	readQueryStart := s.OrderedRead(&table, rawSchema.Columns(), SortAsc, abstract.NoFilter, All, excludeDescendants) + " limit 1000"
	readQueryEnd := s.OrderedRead(&table, rawSchema.Columns(), SortDesc, abstract.NoFilter, All, excludeDescendants) + " limit 1000"
	totalQ := fmt.Sprintf(`
select * from (%v) as top
union
select * from (%v) as bot
`, readQueryStart, readQueryEnd)

	if err := s.loadSample(tx, totalQ, table, rawSchema, startTime, pusher); err != nil {
		return xerrors.Errorf("unable to load sample: %w", err)
	}
	return nil
}

func (s *Storage) TableSizeInBytes(table abstract.TableID) (uint64, error) {
	tinfo, err := s.getLoadTableMode(context.TODO(), abstract.TableDescription{
		Name:   table.Name,
		Schema: table.Namespace,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	})
	if err != nil {
		return 0, xerrors.Errorf("unable to build table info: %w", err)
	}
	if tinfo.tableInfo.HasSubclass || tinfo.tableInfo.IsInherited {
		var size uint64
		if err := s.Conn.QueryRow(context.TODO(), `
SELECT COALESCE(sum(pg_relation_size(inhrelid)), pg_table_size($1))
FROM pg_inherits
WHERE
  inhparent :: regclass = $1 :: regclass;
`, table.Fqtn()).Scan(&size); err != nil {
			return 0, xerrors.Errorf("unable to select sub-tables size: %w", err)
		}
		return size, nil
	}
	var size uint64
	if err := s.Conn.QueryRow(context.TODO(), "select pg_table_size($1);", table.Fqtn()).Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}

func ParseLsn(lsn string) uint64 {
	var upperHalf uint64
	var lowerHalf uint64
	var nparsed int
	nparsed, err := fmt.Sscanf(lsn, "%X/%X", &upperHalf, &lowerHalf)
	if err != nil {
		return 0
	}

	if nparsed != 2 {
		return 0
	}

	outputLsn := (upperHalf << 32) + lowerHalf
	return outputLsn
}

// go-sumtype:decl StorageOpt
type StorageOpt interface {
	isStorageOpt()
}

type (
	Metrics     struct{ metrics.Registry }
	TypeMapping struct{ TypeNameToOIDMap }
)

func (m Metrics) isStorageOpt()     {}
func (m TypeMapping) isStorageOpt() {}

func WithMetrics(registry metrics.Registry) StorageOpt {
	return Metrics{Registry: registry}
}

func WithTypeMapping(typeMapping TypeNameToOIDMap) StorageOpt {
	return TypeMapping{TypeNameToOIDMap: typeMapping}
}

func (s *Storage) RunSlotMonitor(ctx context.Context, serverSource interface{}, registry metrics.Registry, tracker ...*Tracker) (abstract.SlotKiller, <-chan error, error) {
	if pgSrc, ok := serverSource.(*PgSource); ok {
		return RunSlotMonitor(ctx, pgSrc, registry, tracker...)
	} else {
		logger.Log.Error("pkg/storage/slot_monitor get not server.PgSource as server.Source")
		return nil, nil, xerrors.Errorf("pkg/storage/slot_monitor get not server.PgSource as server.Source")
	}
}

// Named BeginPGSnapshot to NOT match abstract.SnapshotableStorage
func (s *Storage) BeginPGSnapshot(ctx context.Context) error {
	rb := util.Rollbacks{}
	defer rb.Do()

	var err error
	s.snapshotConnection, err = s.Conn.Acquire(ctx)
	if err != nil {
		return err
	}
	rb.Add(func() {
		s.snapshotConnection.Release()
		s.snapshotConnection = nil
	})

	skipSetSnapshot := shouldSkipSetSnapshot(ctx, s.snapshotConnection.Conn(), logger.Log)

	s.snapshotTransaction, err = s.snapshotConnection.Begin(ctx)
	if err != nil {
		return xerrors.Errorf("failed to BEGIN a transaction: %w", err)
	}
	// it should be after Begin in the same tx. See: MDBSUPPORT-4153 & DTSUPPORT-532
	_, err = s.snapshotTransaction.Exec(ctx, "set session idle_in_transaction_session_timeout = '0'")
	if err != nil {
		return xerrors.Errorf("failed to set idle_in_transaction_session_timeout = 0: %w", err)
	}

	if !skipSetSnapshot {
		logger.Log.Infof("Setting snapshot on host %v", s.snapshotConnection.Conn().Config().Host)
		var commitTime time.Time
		if err = s.snapshotTransaction.QueryRow(ctx, "SELECT now()::TIMESTAMP WITH TIME ZONE as dt, pg_export_snapshot() as lsn").Scan(&commitTime, &s.ShardedStateLSN); err != nil {
			return xerrors.Errorf("failed to call pg_export_snapshot(): %w", err)
		}
		s.ShardedStateTS = commitTime.Round(1 * time.Millisecond)
		logger.Log.Infof("Snapshot set successfully with lsn %v at %v", s.SnapshotLSN(), s.ShardedStateTS)
	} else {
		logger.Log.Warn("this database does not support SET TRANSACTION SNAPSHOT. Current transfer provides per-table consistency only: different tables are copied in different transactions")
	}

	rb.Cancel()
	return nil
}

// shouldSkipSetSnapshot is a special method providing a workaround for AWS RDS Aurora-PostgreSQL database. This database provides PostgreSQL interface but does not support `SET TRANSACTION SNAPSHOT`, so snapshot manipulations should be skipped.
func shouldSkipSetSnapshot(ctx context.Context, conn *pgx.Conn, lgr log.Logger) bool {
	return isAurora(ctx, conn, lgr)
}

func isAurora(ctx context.Context, conn pgxtype.Querier, lgr log.Logger) bool {
	var rdsAdminDBExists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT * FROM pg_database WHERE datname = 'rdsadmin')").Scan(&rdsAdminDBExists); err != nil {
		lgr.Warn("failed to check existence of the 'rdsadmin' database to check whether snapshot needs to be set", log.Error(err))
		return false
	}
	if !rdsAdminDBExists {
		return false
	}
	var auroraVersion string
	if err := conn.QueryRow(ctx, "SELECT aurora_version()").Scan(&auroraVersion); err != nil {
		lgr.Warn("failed to execute 'aurora_version()', although the 'rdsadmin' database exists", log.Error(err))
		return false
	}
	return true
}

// Named EndPGSnapshot to NOT match abstract.SnapshotableStorage
func (s *Storage) EndPGSnapshot(ctx context.Context) error {
	if s.snapshotConnection == nil || s.snapshotTransaction == nil {
		return nil
	}
	if err := s.snapshotTransaction.Rollback(context.TODO()); err != nil {
		return xerrors.Errorf("failed to ROLLBACK the snapshot transaction: %w", err)
	}
	s.snapshotTransaction = nil

	s.snapshotConnection.Release()
	s.snapshotConnection = nil

	return nil
}

func (s *Storage) TableAccessible(table abstract.TableDescription) bool {
	row := s.Conn.QueryRow(context.TODO(), checkAccessibilityQuery(&table))

	var check int
	if err := row.Scan(&check); err != nil && err != pgx.ErrNoRows {
		logger.Log.Warnf("Inaccessible table %v: %v", table.Fqtn(), err)
		return false
	}
	return true
}

func (s *Storage) BuildTypeMapping(ctx context.Context) (TypeNameToOIDMap, error) {
	rows, err := s.Conn.Query(ctx, typeMapQuery())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	typeMap := make(TypeNameToOIDMap)
	for rows.Next() {
		var typeFullName TypeFullName
		var oid pgtype.OID
		if err := rows.Scan(&typeFullName.Namespace, &typeFullName.Name, &oid); err != nil {
			return nil, xerrors.Errorf("scan: %w", err)
		}
		typeMap[typeFullName] = oid
		logger.Log.Debugf("Map type %s.%s -> OID %d", typeFullName.Namespace, typeFullName.Name, oid)
	}
	if rows.Err() != nil {
		return nil, xerrors.Errorf("error while reading response: %w", err)
	}

	return typeMap, nil
}

func (s *Storage) LoadQueryTable(ctx context.Context, tableQuery tablequery.TableQuery, pusher abstract.Pusher) error {
	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return xerrors.Errorf("failed to acquire a connection from the pool: %w", err)
	}
	defer conn.Release()

	tableDescription := abstract.TableDescription{
		Name:   tableQuery.TableID.Name,
		Schema: tableQuery.TableID.Namespace,
		EtaRow: 0,
		Filter: "",
		Offset: 0,
	}

	loadMode, err := s.discoverTableLoadMode(ctx, conn, tableDescription)
	if err != nil {
		return xerrors.Errorf("failed to discover table %v loading mode: %w", tableDescription.Fqtn(), err)
	}

	excludeDescendants, reason := loadMode.ExcludeDescendants()
	if excludeDescendants {
		logger.Log.Infof("Use ONLY statement for selecting from table %v: %v", tableDescription.Fqtn(), reason)
	}

	schema, err := s.LoadSchemaForTable(ctx, conn.Conn(), tableDescription)
	if err != nil {
		return xerrors.Errorf("failed to get schema for table %s: %w", tableQuery.TableID.Fqtn(), err)
	}

	query := s.queryFromQueryTable(&tableQuery, schema, SortAsc, abstract.NoFilter, All, excludeDescendants)

	logger.Log.Infof("Storage::LoadQueryTable - built query: %s", query)

	if err = s.loadTable(ctx, tableDescription, pusher, query, conn.Conn(), loadMode, schema, time.Now()); err != nil {
		return xerrors.Errorf("unable to load table : %w", err)
	}

	return nil
}

func (s *Storage) queryFromQueryTable(
	tableQuery *tablequery.TableQuery,
	tableSchema *abstract.TableSchema,
	sortOrder SortOrder,
	filter abstract.WhereStatement,
	duplicatesPolicy DuplicatesPolicy,
	excludeDescendants bool,
) string {
	var fromStatement string

	if excludeDescendants {
		fromStatement = fmt.Sprintf("only %v", tableQuery.TableID.Fqtn())
	} else {
		fromStatement = tableQuery.TableID.Fqtn()
	}

	keyCols := make([]string, 0)
	cols := make([]string, 0)

	for _, col := range tableSchema.Columns() {
		if s.Config.IgnoreUserTypes {
			if IsUserDefinedType(&col) {
				continue
			}
		}
		if col.PrimaryKey && col.ColumnName != "" {
			keyCols = append(keyCols, fmt.Sprintf("\"%v\"", col.ColumnName))
		}
		if col.OriginalType == "pg:hstore" && !s.IsHomo {
			cols = append(cols, fmt.Sprintf("\"%v\"::json", col.ColumnName))
		} else {
			cols = append(cols, fmt.Sprintf("\"%v\"", col.ColumnName))
		}
	}

	if len(keyCols) == 0 {
		return fmt.Sprintf("select * from %s where %s", tableQuery.TableID.Fqtn(), WhereClause(tableQuery.Filter))
	}

	q := fmt.Sprintf(
		"select %v %v from %s where (%s) and (%s)",
		duplicatesPolicy,
		strings.Join(cols, ","),
		fromStatement,
		WhereClause(tableQuery.Filter),
		WhereClause(filter),
	)
	if tableQuery.Offset != 0 || tableQuery.SortByPKeys {
		if tableQuery.Offset != 0 {
			q += fmt.Sprintf(
				" order by %s %s  OFFSET %v",
				strings.Join(keyCols, ","),
				sortOrder,
				tableQuery.Offset,
			)
		} else {
			q += fmt.Sprintf(
				" order by %s %s",
				strings.Join(keyCols, ","),
				sortOrder,
			)
		}
	}
	if tableQuery.Limit != 0 {
		q += fmt.Sprintf(
			" LIMIT %v",
			tableQuery.Limit)
	}

	return q
}

func (s *Storage) loadTable(
	ctx context.Context,
	table abstract.TableDescription,
	pusher abstract.Pusher,
	readQuery string,
	conn *pgx.Conn,
	loadMode *loadTableMode,
	schema *abstract.TableSchema,
	startTime time.Time,
) error {
	if skip, reason := loadMode.SkipLoading(); skip {
		logger.Log.Infof("Skip load table %v: %v", table.Fqtn(), reason)
		return nil
	}

	if _, err := conn.Exec(ctx, MakeSetSQL("statement_timeout", "0")); err != nil {
		return xerrors.Errorf("failed to SET statement_timeout: %w", err)
	}

	params := readQueryParams(s.Config.UseBinarySerialization, len(schema.Columns()))
	rows, err := conn.Query(ctx, readQuery, params...)
	if err != nil {
		return xerrors.Errorf("failed to execute SELECT: %w", err)
	}
	defer rows.Close()

	ciFetcher := NewChangeItemsFetcher(rows, conn, abstract.ChangeItem{
		ID:           uint32(0),
		LSN:          0,
		CommitTime:   uint64(startTime.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       table.Schema,
		Table:        table.Name,
		PartID:       table.PartID(),
		ColumnNames:  schema.Columns().ColumnNames(),
		ColumnValues: nil,
		TableSchema:  schema,
		OldKeys:      abstract.EmptyOldKeys(),
		TxID:         "",
		Query:        "",
		Size:         abstract.EmptyEventSize(),
	}, s.metrics).WithUnmarshallerData(MakeUnmarshallerData(s.IsHomo, conn))

	totalRowsRead := uint64(0)
	defer func() {
		logger.Log.Info(fmt.Sprintf("In total, %d rows were read from table", totalRowsRead), log.String("table", table.String()))
	}()

	logger.Log.Info("extracting data from the source table...", log.String("fqtn", table.Fqtn()))
	for ciFetcher.MaybeHasMore() {
		items, err := ciFetcher.Fetch()
		if err != nil {
			return xerrors.Errorf("failed to extract data from the table %s: %w", table.Fqtn(), err)
		}
		if len(items) > 0 {
			totalRowsRead += uint64(len(items))
			s.metrics.ChangeItems.Add(int64(len(items)))
			if err := pusher(items); err != nil {
				return xerrors.Errorf("failed to push %d ChangeItems. Error: %w", len(items), err)
			}
		}
	}
	logger.Log.Info("successfully extracted data from the source table", log.String("fqtn", table.Fqtn()))

	return nil
}

func NewStorage(config *PgStorageParams, opts ...StorageOpt) (*Storage, error) {
	var err error
	var connConfig *pgx.ConnConfig

	if config.PreferReplica {
		// 'PreferReplica' automatically auto-derived into 'true', when: managed installation & heterogeneous SNAPSHOT_ONLY transfer
		logger.Log.Info("Prefer replica is on, will try to find alive and up-to-date replica")
	}

	connConfig, err = MakeConnConfigFromStorage(logger.Log, config)
	if err != nil {
		return nil, err
	}
	logger.Log.Info("Host chosen", log.String("pg_host", connConfig.Host), log.UInt16("pg_port", connConfig.Port))

	poolConfig, _ := pgxpool.ParseConfig(config.ConnString)
	poolConfig.ConnConfig = WithLogger(connConfig, log.With(logger.Log, log.Any("component", "pgx")))
	poolConfig.MaxConns = 10

	currMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	var typeNameToOID TypeNameToOIDMap

	for _, opt := range opts {
		switch option := opt.(type) {
		case Metrics:
			currMetrics = stats.NewSourceStats(option.Registry)
		case TypeMapping:
			typeNameToOID = option.TypeNameToOIDMap
		}
	}

	var dataTypesOptions []DataTypesOption
	if config.UseBinarySerialization {
		genericBinaryType := (*pgtype.GenericBinary)(nil)
		dataTypesOptions = append(dataTypesOptions, &DefaultDataType{Value: genericBinaryType})
	}
	if typeNameToOID != nil {
		dataTypesOptions = append(dataTypesOptions, typeNameToOID)
	}
	poolConfig.AfterConnect = MakeInitDataTypes(dataTypesOptions...)

	conn, err := NewPgConnPoolConfig(context.TODO(), poolConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to make a connection pool: %w", err)
	}

	version := ResolveVersion(conn)

	storage := &Storage{
		Config:              config,
		Conn:                conn,
		version:             version,
		snapshotConnection:  nil,
		snapshotTransaction: nil,
		IsHomo:              false,
		once:                sync.Once{},
		metrics:             currMetrics,
		ForbiddenSchemas:    pgSystemSchemas(),
		ForbiddenTables:     pgSystemTableNames(),
		Flavour:             NewPostgreSQLFlavour(),
		typeNameToOID:       typeNameToOID,
		ShardedStateLSN:     "",
		ShardedStateTS:      time.Time{},
		loadDescending:      false,
		sExTime:             time.Time{},
	}
	return storage, nil
}
