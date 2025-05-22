package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/set"
	"github.com/transferia/transferia/pkg/util/size"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/maps"
)

var (
	systemDBs            = set.New("INFORMATION_SCHEMA", "information_schema", "system", "_system")
	nonTransferableTypes = []string{
		// For now we do not support next types:
		//   Array(Date)
		//   Array(Date32)
		//   Array(DateTime)
		//   Array(DateTime64)
		//   Array(DateTime64)
		//   Array(DateTime64)
		//   Array(Nullable(Date))
		//   Array(Nullable(Date32))
		//   Array(Nullable(DateTime))
		//   Array(Nullable(DateTime64))
		//   Array(Nullable(DateTime64))
		//   Array(Nullable(DateTime64))
		// ClickHouse driver incorrectly reads the time zone for these types.
		"Array(Date",
		"Array(Nullable(Date",
	}
)

type ClickhouseStorage interface {
	abstract.SampleableStorage
	LoadTablesDDL(tables []abstract.TableID) ([]schema.TableDDL, error)
	BuildTableQuery(table abstract.TableDescription) (*abstract.TableSchema, string, string, error)
	GetRowsCount(tableID abstract.TableID) (uint64, error)
	TableParts(ctx context.Context, table abstract.TableID) ([]TablePart, error)
	Version() semver.Version
}

var _ ClickhouseStorage = (*Storage)(nil)

type Storage struct {
	db        *sql.DB
	bufSize   uint64
	logger    log.Logger
	onceClose sync.Once
	IsHomo    bool
	metrics   *stats.SourceStats
	version   semver.Version
	database  string
	cluster   string

	tableFilter           abstract.Includeable
	allowSingleHostTables bool
}

type TablePart struct {
	Table      abstract.TableID
	Name       string // clickhouse `_partiotion_id` virtual columnt
	Rows       int64
	Bytes      int64
	Shard      string
	ShardCount int
}

func (p TablePart) Key() string {
	return fmt.Sprintf("%v.%v.%v", p.Shard, p.Table.Fqtn(), p.Name)
}

func (s *Storage) TableParts(ctx context.Context, table abstract.TableID) ([]TablePart, error) {
	var res []TablePart
	rows, err := s.db.QueryContext(ctx, `
select partition_id, sum(bytes) as bytes, sum(rows) as rows from system.parts where database = ? and table = ? and active
group by partition_id
order by bytes desc; -- take fat part first
`, table.Namespace, table.Name)
	if err != nil {
		return nil, xerrors.Errorf("unable to query parts rows: %w", err)
	}
	defer rows.Close()
	totalBytes := int64(0)
	for rows.Next() {
		var part TablePart
		if err := rows.Scan(&part.Name, &part.Bytes, &part.Rows); err != nil {
			return nil, xerrors.Errorf("unable to scan part row: %w", err)
		}
		totalBytes += part.Bytes
		part.Table = table
		part.ShardCount = 1
		res = append(res, part)
	}
	if len(res) == 0 || totalBytes < size.GiB {
		// we have no parts, or table small enough - add table with no part filter
		return []TablePart{{
			Name:       "",
			Rows:       0,
			Bytes:      0,
			Shard:      "",
			Table:      table,
			ShardCount: 1,
		}}, nil
	}
	return res, nil
}

func (s *Storage) Version() semver.Version {
	return s.version
}

func (s *Storage) Close() {
	s.onceClose.Do(func() {
		if err := s.db.Close(); err != nil {
			s.logger.Warnf("unable to close DB connector: %v", err)
		}
	})
}

func (s *Storage) Ping() error {
	return s.db.Ping()
}

func (s *Storage) CopySchema(tables abstract.TableMap, pusher abstract.Pusher) error {
	tIDs := make([]abstract.TableID, len(tables))
	i := 0
	for tID := range tables {
		tIDs[i] = tID
		i++
	}
	createDDLs, err := s.LoadTablesDDL(tIDs)
	if err != nil {
		return xerrors.Errorf("unable to load create table DDL: %w", err)
	}

	for _, ddl := range createDDLs {
		if err := pusher([]abstract.ChangeItem{ddl.ToChangeItem()}); err != nil {
			return xerrors.Errorf("unable to apply ddl(%v): %w", ddl.SQL(), err)
		}
	}
	return nil
}

func (s *Storage) isView(table abstract.TableDescription) bool {
	engineRow := s.db.QueryRow(`
                SELECT engine
                FROM system.tables
                WHERE database = ? AND name = ?;
        `, table.Schema, table.Name)
	var engine string
	if err := engineRow.Scan(&engine); err != nil {
		s.logger.Warnf("%v was not found in system.tables: %v", table.Fqtn(), err)
		return false
	}
	return engine == "MaterializedView"
}

func (s *Storage) checkTypes(columns abstract.TableColumns) error {
	errors := util.NewErrs()
	for _, column := range columns {
		columnType := strings.TrimPrefix(column.OriginalType, "ch:")
		for _, nonTransferableType := range nonTransferableTypes {
			if strings.HasPrefix(columnType, nonTransferableType) {
				errors = util.AppendErr(errors, xerrors.Errorf("Can't transfer type '%v', column '%v'", columnType, column.ColumnName))
			}
		}
	}
	if !errors.Empty() {
		return errors
	}
	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	tableAllColumns, _, err := s.getTableSchema(table, s.IsHomo)
	if err != nil {
		return nil, xerrors.Errorf("unable to get table schema: %w", err)
	}

	return tableAllColumns, nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	if s.isView(table) {
		s.logger.Infof("skip view %v", table.Fqtn())
		return nil
	}
	timestampTz := util.GetTimestampFromContextOrNow(ctx)
	commitTime := uint64(timestampTz.UnixNano())

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return xerrors.Errorf("unable to open connection to storage: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Warnf("unable to release connection: %v", err)
		}
	}()

	//get tables schema []abstract.ColSchema (as in ListTable)
	tableAllColumns, tableFilteredColumns, err := s.getTableSchema(table.ID(), s.IsHomo)
	if err != nil {
		return xerrors.Errorf("unable to discover table schema in storage: %w", err)
	}
	deletable, err := s.isDeletable(table.ID())
	if err != nil {
		return xerrors.Errorf("failed to determine deletable table %s: %w", table.Fqtn(), err)
	}
	if err := s.checkTypes(tableFilteredColumns.Columns()); err != nil {
		return xerrors.Errorf("Can't transfer table %s: %w", table.Fqtn(), err)
	}

	chunkSize := s.inferDesiredChunkSize(table)

	//build read query with filter and offset
	readQ := buildSelectQuery(&table, tableAllColumns.Columns(), s.IsHomo, deletable, "")
	s.logger.Info("built select query", log.Any("query", readQ))

	rows, err := conn.QueryContext(ctx, readQ)
	if err != nil {
		s.logger.Error("rows select error", log.Error(err), log.Any("query", util.Sample(readQ, 1024)))
		return xerrors.Errorf("unable to select rows from table %v: %w", table.Fqtn(), err)
	}
	defer rows.Close()

	// do upload
	reader := &rowsReader{
		table:                table,
		rows:                 rows,
		ts:                   commitTime,
		inflightBytesLimit:   s.bufSize,
		chunkSize:            chunkSize,
		tableAllColumns:      tableAllColumns,
		tableFilteredColumns: tableFilteredColumns,
	}
	if err := s.readRowsAndPushByChunks(reader, pusher, table); err != nil {
		return xerrors.Errorf("unable to upload table %v: %w", table.Fqtn(), err)
	}

	return nil
}

type rowsReader struct {
	table abstract.TableDescription
	rows  *sql.Rows
	ts    uint64

	chunkSize          uint64
	inflightBytesLimit uint64

	tableAllColumns      *abstract.TableSchema
	tableFilteredColumns *abstract.TableSchema // when IsHomo - filter out alias & materialized
}

func (s *Storage) readRowsAndPushByChunks(reader *rowsReader, pusher abstract.Pusher, tDescr abstract.TableDescription) error {
	rowValues, err := InitValuesForScan(reader.rows)
	if err != nil {
		return xerrors.Errorf("unable to init values for scan result: %w", err)
	}

	colsNames := make([]string, len(reader.tableFilteredColumns.Columns()))
	for i, col := range reader.tableFilteredColumns.Columns() {
		colsNames[i] = col.ColumnName
	}

	partID := tDescr.PartID()

	rowsCount := uint64(0)
	inflightBytes := uint64(0)
	inflight := make([]abstract.ChangeItem, 0)
	for reader.rows.Next() {
		err := reader.rows.Scan(rowValues...)
		if err != nil {
			return xerrors.Errorf("unable to scan row from result: %w", err)
		}

		values, bytes := MarshalFields(rowValues, reader.tableFilteredColumns.Columns())

		inflight = append(inflight, abstract.ChangeItem{
			CommitTime:   reader.ts,
			Kind:         abstract.InsertKind,
			Schema:       reader.table.Schema,
			Table:        reader.table.Name,
			PartID:       partID,
			ColumnNames:  colsNames,
			ColumnValues: values,
			TableSchema:  reader.tableAllColumns,
			ID:           0,
			LSN:          0,
			Counter:      0,
			OldKeys:      abstract.EmptyOldKeys(),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(bytes),
		})
		rowsCount++
		inflightBytes += bytes

		s.metrics.Size.Add(int64(bytes))
		if uint64(len(inflight)) >= reader.chunkSize || inflightBytes >= reader.inflightBytesLimit {
			s.logger.Infof("ready to push: %v rows, buffer size %v", uint64(len(inflight)), humanize.BigBytes(big.NewInt(int64(inflightBytes))))
			if err := pusher(inflight); err != nil {
				s.logger.Errorf("unable to push %v rows to %v: %v", uint64(len(inflight)), reader.table.Fqtn(), err)
				return xerrors.Errorf("unable to push last %d rows to table %s: %w", uint64(len(inflight)), reader.table.Fqtn(), err)
			}
			inflight = make([]abstract.ChangeItem, 0)
			inflightBytes = 0
		}
	}
	if err := reader.rows.Err(); err != nil {
		return xerrors.Errorf("unable to read data rows: %w", err)
	}
	if len(inflight) > 0 {
		if err := pusher(inflight); err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) inferDesiredChunkSize(table abstract.TableDescription) uint64 {
	defaultSize := uint64(5000)
	minSize := 1000

	rows, bytes, err := s.getTableSize(table.ID())
	if err != nil {
		s.logger.Warnf("unable to discover desired chunk size by actual table size: %v", err)
		return defaultSize
	}
	if bytes == 0 || rows == 0 {
		return uint64(minSize)
	}
	calculatedSize := int(float64(s.bufSize) / (float64(bytes) / float64(rows)))
	chunkSize := calculatedSize
	if bytes > 0 && calculatedSize < minSize {
		chunkSize = minSize
	} else {
		chunkSize = calculatedSize
	}
	s.logger.Infof(
		"Infer rows chunk size for table(%v) = %v, table rows = %v, table size = %v, inflight buffer bytes limit = %v",
		table.Fqtn(),
		chunkSize,
		rows,
		humanize.BigBytes(big.NewInt(int64(bytes))),
		humanize.BigBytes(big.NewInt(int64(s.bufSize))))
	return uint64(chunkSize)
}

// in fact - it's estimate, and it's enough to be estimate
// works only for 20+ clickhouse version
func (s *Storage) getTableSize(tableID abstract.TableID) (rows uint64, bytes uint64, err error) {
	if s.version.Major < 20 {
		return 0, 0, nil
	}

	var r, b *uint64
	err = s.db.QueryRow(`
		SELECT total_rows, total_bytes
		FROM system.tables
		WHERE database = ? AND name = ?;
	`, tableID.Namespace, tableID.Name).Scan(&r, &b)

	if r != nil {
		rows = *r
	}
	if b != nil {
		bytes = *b
	}

	if err == nil && rows == 0 {
		logger.Log.Warnf("table %s has 0 rows in system.tables", tableID.Fqtn())
	}
	return rows, bytes, err
}

func (s *Storage) GetRowsCount(tableID abstract.TableID) (uint64, error) {
	var rows uint64
	var err error
	deletable, err := s.isDeletable(tableID)
	if err != nil {
		return 0, xerrors.Errorf("failed to determine isDeletable storage or not for table %s: %w", tableID.Fqtn(), err)
	}
	if deletable || s.version.Major < 20 {
		maybeFinal := ""
		if deletable {
			maybeFinal = "FINAL"
		}
		query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` %s WHERE 1=1 %s;", tableID.Namespace, tableID.Name, maybeFinal, getDeleteTimeFilterExpr(deletable))
		res := s.db.QueryRow(query)
		err = res.Scan(&rows)
		if err != nil {
			return 0, xerrors.Errorf("failed to select query \"%s\" for table %s: %w", query, tableID.Fqtn(), err)
		}
	} else {
		rows, _, err = s.getTableSize(tableID)
		if err != nil {
			return 0, xerrors.Errorf("failed to get rows count for table %s: %w", tableID.Fqtn(), err)
		}
	}
	return rows, nil
}

type table struct {
	database    string
	name        string
	rowsCount   uint64
	primaryKeys []string
	isView      bool
}

type tables []table

func (ts tables) String() string {
	result := make([]string, 0)
	for _, table := range ts {
		result = append(result, table.ToTableID().Fqtn())
	}
	return strings.Join(result, ", ")
}

func (t *table) ToTableID() abstract.TableID {
	return abstract.TableID{
		Namespace: t.database,
		Name:      t.name,
	}
}

func (s *Storage) BuildTableQuery(table abstract.TableDescription) (*abstract.TableSchema, string, string, error) {
	//get tables schema []abstract.ColSchema (as in ListTable)
	tableAllColumns, readCols, err := s.getTableSchema(table.ID(), s.IsHomo)
	if err != nil {
		return nil, "", "", xerrors.Errorf("unable to discover table schema in storage: %w", err)
	}
	deletable, err := s.isDeletable(table.ID())
	if err != nil {
		return nil, "", "", xerrors.Errorf("failed to determine deletable table %s: %w", table.Fqtn(), err)
	}
	//build read query with filter and offset
	readQ := buildSelectQuery(&table, tableAllColumns.Columns(), s.IsHomo, deletable, "")
	s.logger.Info("built select query", log.Any("query", readQ))
	countQ := buildCountQuery(&table, deletable, "")
	return readCols, readQ, countQ, nil
}

func (s *Storage) listTables(schema, name string) ([]table, error) {
	showAllSchemas := "y"
	if schema != "*" {
		showAllSchemas = "n"
	}
	showAllTables := "y"
	if name != "*" {
		showAllTables = "n"
	}
	query := `
	SELECT database, name, total_rows, primary_key, engine
	FROM system.tables
	WHERE is_temporary = 0 AND (database = ? OR 'y' = ?) AND (name = ? OR 'y' = ?);
`

	if s.version.Major < 20 {
		query = `
	SELECT database, name, toUInt64(0) as total_rows, primary_key, engine
	FROM system.tables
	WHERE is_temporary = 0 AND (database = ? OR 'y' = ?) AND (name = ? OR 'y' = ?);
`
	}
	tablesRes, err := s.db.Query(query, schema, showAllSchemas, name, showAllTables)
	if err != nil {
		return nil, xerrors.Errorf("unable to list tables: %w", err)
	}
	defer tablesRes.Close()

	tables := make([]table, 0)
	for tablesRes.Next() {
		var database, name, pkeys, engine string
		var totalRows *uint64 // total_rows could be null
		var rows uint64

		if err := tablesRes.Scan(&database, &name, &totalRows, &pkeys, &engine); err != nil {
			return nil, xerrors.Errorf("unable to parse table list query result: %w", err)
		}

		if systemDBs.Contains(database) {
			continue
		}
		if totalRows != nil {
			rows = *totalRows
		}
		tables = append(tables, table{
			database:    database,
			name:        name,
			rowsCount:   rows,
			primaryKeys: s.parsePkeys(pkeys),
			isView:      engine == "MaterializedView",
		})
	}
	if err := tablesRes.Err(); err != nil {
		return nil, xerrors.Errorf("unable to read rows from system.tables: %w", err)
	}
	s.logger.Infof("found %v tables in storage", len(tables))
	return tables, nil
}

func (s *Storage) parsePkeys(raw string) []string {
	stripped := strings.ReplaceAll(raw, " ", "")
	if stripped == "" {
		return nil
	}
	return strings.Split(stripped, ",")
}

func (s *Storage) discoverTableSchema(t table) (*abstract.TableInfo, error) {
	cols, err := backoff.RetryWithData[*abstract.TableSchema](func() (*abstract.TableSchema, error) {
		return schema.DescribeTable(s.db, t.database, t.name, t.primaryKeys)
	}, backoff.NewExponentialBackOff())
	if err != nil {
		return nil, xerrors.Errorf("unable to describe table: %s: %w", t.ToTableID(), err)
	}

	return &abstract.TableInfo{
		EtaRow: t.rowsCount,
		Schema: cols,
		IsView: t.isView,
	}, nil
}

func (s *Storage) TableList(includeTableFilter abstract.IncludeTableList) (abstract.TableMap, error) {
	allTables, err := s.listTables(s.database, "*")
	if err != nil {
		return nil, xerrors.Errorf("unable to list tables: %w", err)
	}
	s.logger.Info("Extracted tables (unfiltered)", log.String("tables", tables(allTables).String()))

	tableMap := make(abstract.TableMap)
	tableFilter := abstract.NewIntersectionIncludeable(s.tableFilter, includeTableFilter)
	sortedTablesForLog := make([]string, 0)

	for _, table := range allTables {
		if table.database == "system" {
			continue
		}

		id := abstract.TableID{
			Namespace: table.database,
			Name:      table.name,
		}
		if tableFilter != nil && !tableFilter.Include(id) {
			continue
		}

		info, err := s.discoverTableSchema(table)
		if err != nil {
			return tableMap, xerrors.Errorf("unable to discover schema for table %v.%v: %w", table.database, table.name, err)
		}
		tableMap[id] = *info
		sortedTablesForLog = append(sortedTablesForLog, id.Fqtn())
	}
	s.logger.Info("Extracted tables (filtered)", log.String("tables", strings.Join(sortedTablesForLog, ", ")))

	if s.allowSingleHostTables {
		return tableMap, nil
	}
	if err := s.checkTables(tableMap); err != nil {
		return nil, xerrors.Errorf("found unsupported tables: %w", err)
	}
	return tableMap, nil
}

func (s *Storage) checkTables(tables abstract.TableMap) error {
	ddls, err := s.LoadTablesDDL(maps.Keys(tables))
	if err != nil {
		return xerrors.Errorf(": %w", err)
	}

	var badTables []abstract.TableID
	for _, ddl := range ddls {
		// TODO: should check table/view presence on all cluster hosts somehow
		if engine := ddl.Engine(); engine != "Distributed" && !strings.Contains(engine, "Replicated") && !ddl.IsMatView() {
			badTables = append(badTables, ddl.TableID())
		}
	}

	if len(badTables) == 0 {
		return nil
	}
	return xerrors.Errorf("the following tables have not Distributed or Replicated engines and are not yet supported: %s",
		strings.Join(yslices.Map(badTables, func(id abstract.TableID) string { return id.Fqtn() }), ", "))
}

func (s *Storage) getTableSchema(tID abstract.TableID, isHomo bool) (*abstract.TableSchema, *abstract.TableSchema, error) {
	tables, err := backoff.RetryWithData[[]table](func() ([]table, error) {
		return s.listTables(tID.Namespace, tID.Name)
	}, backoff.NewExponentialBackOff())
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to list tables: %w", err)
	}
	if len(tables) == 0 {
		return nil, nil, xerrors.Errorf("no tables were found with fqtn %v.%v", tID.Namespace, tID.Name)
	}

	tInfo, err := s.discoverTableSchema(tables[0])
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to discover schema for table %s: %w", tID.Fqtn(), err)
	}

	filteredColumns := make(abstract.TableColumns, 0)
	for _, col := range tInfo.Schema.Columns() {
		if ColumnShouldBeSelected(col, isHomo) {
			filteredColumns = append(filteredColumns, col)
		}
	}
	return tInfo.Schema, abstract.NewTableSchema(filteredColumns), nil
}

func mapKeys(set map[string]bool) []string {
	res := make([]string, len(set))
	i := 0
	for k := range set {
		res[i] = k
		i++
	}
	return res
}

func makeFilters(tables []abstract.TableID) (string, string) {
	dbSet := make(map[string]bool)
	nameSet := make(map[string]bool)
	for _, t := range tables {
		dbSet[t.Namespace] = true
		nameSet[t.Name] = true
	}
	dbs := fmt.Sprintf("('%v')", strings.Join(mapKeys(dbSet), "','"))
	names := fmt.Sprintf("('%v')", strings.Join(mapKeys(nameSet), "','"))
	return dbs, names
}

func (s *Storage) LoadTablesDDL(tables []abstract.TableID) ([]schema.TableDDL, error) {
	dbFilter, nameFilter := makeFilters(tables)
	q := fmt.Sprintf("select database, name, create_table_query, engine from system.tables where database in %v and name in %v", dbFilter, nameFilter)
	foundDdls := make(map[abstract.TableID]schema.TableDDL)
	if err := backoff.Retry(func() error {
		tablesRes, err := s.db.Query(q)
		if err != nil {
			return xerrors.Errorf("unable to list create tables query: %w", err)
		}
		defer tablesRes.Close()
		for tablesRes.Next() {
			var database, name, createDDL, engine string

			if err := tablesRes.Scan(&database, &name, &createDDL, &engine); err != nil {
				return xerrors.Errorf("unable to parse create tables ddls query result: %w", err)
			}
			tID := abstract.TableID{
				Namespace: database,
				Name:      name,
			}
			foundDdls[tID] = schema.NewTableDDL(
				abstract.TableID{Namespace: database, Name: name},
				createDDL,
				engine,
			)
		}
		if err := tablesRes.Err(); err != nil {
			return xerrors.Errorf("unable to read rows from system.tables: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5)); err != nil {
		return nil, xerrors.Errorf("unable to load table DDL: %w", err)
	}

	ddls := make([]schema.TableDDL, 0)
	missedTables := make([]string, 0)
	materializedViews := make([]schema.TableDDL, 0)
	for _, tID := range tables {
		ddl, ok := foundDdls[tID]
		if !ok {
			missedTables = append(missedTables, tID.Fqtn())
		} else {
			if ddl.IsMatView() {
				materializedViews = append(materializedViews, ddl)
			} else {
				ddls = append(ddls, ddl)
			}
		}
	}
	ddls = append(ddls, materializedViews...) // order matters bcs mat views depend on base tables
	if len(missedTables) > 0 {
		return nil, xerrors.Errorf("for some tables create table query was not found: %v", missedTables)
	}
	return ddls, nil
}

func MakeConnection(cfg *model.ChStorageParams) (*sql.DB, error) {
	return makeShardConnection(cfg, "")
}

func makeShardConnection(cfg *model.ChStorageParams, shardName string) (*sql.DB, error) {
	hosts, err := model.ConnectionHosts(cfg, shardName)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve ClickHouse configuration to a list of hosts: %w", err)
	}
	return conn.ConnectNative(hosts[0], cfg.ToConnParams())
}

func (s *Storage) LoadSchema() (dbSchema abstract.DBSchema, err error) {
	tableMap, err := s.TableList(nil)
	if err != nil {
		return nil, err
	}
	return tableMap.ToDBSchema(), nil
}

func (s *Storage) isDistributed(table abstract.TableID) (bool, error) {
	engineRow := s.db.QueryRow(`SELECT engine FROM system.tables WHERE database=? AND name=?;`, table.Namespace, table.Name)
	var engine string
	if err := engineRow.Scan(&engine); err != nil && err != sql.ErrNoRows {
		return false, xerrors.Errorf("error while getting engine of table %s : %w", table.Fqtn(), err)
	}
	return engine == "Distributed", nil
}

// isDeletable check where this table created by transfer replication.
//
//	Table that have `__data_transfer_delete_time` contains transfer soft-deletes should be treated as deletable
func (s *Storage) isDeletable(table abstract.TableID) (bool, error) {
	return backoff.RetryWithData[bool](func() (bool, error) {
		row := s.db.QueryRow(
			`select count() > 0 from system.columns where database=? AND table=? and name = ?;`,
			table.Namespace,
			table.Name,
			"__data_transfer_delete_time",
		)
		var exist bool
		if err := row.Scan(&exist); err != nil {
			return false, xerrors.Errorf("error while getting engine of table %s : %w", table.Fqtn(), err)
		}
		return exist, nil
	}, backoff.NewExponentialBackOff())
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, xerrors.New("not implemented")
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	deletable, err := s.isDeletable(table)
	if err != nil {
		return 0, xerrors.Errorf("failed to determine engine for table %s: %w", table, err)
	}
	finalStr := ""
	if deletable {
		finalStr = "FINAL"
	}

	q := fmt.Sprintf(queryExactRows, table.Fqtn(), finalStr, getDeleteTimeFilterExpr(deletable))
	row := s.db.QueryRow(q)

	var rowsCount uint64
	if err := row.Scan(&rowsCount); err != nil {
		return 0, xerrors.Errorf("failed to get rows count:%w", err)
	}
	return rowsCount, nil
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	row := s.db.QueryRow(fmt.Sprintf("EXISTS `%s`.`%s`;", table.Namespace, table.Name))

	var exists uint8
	if err := row.Scan(&exists); err != nil {
		return false, xerrors.Errorf("failed to check is table exists:%w", err)
	}
	return exists == 1, nil
}

func getDeleteTimeFilterExpr(presentDTDeleteTime bool) string {
	if presentDTDeleteTime {
		return " AND __data_transfer_delete_time==0"
	} else {
		return ""
	}
}

type StorageOpt func(storage *Storage) *Storage

func WithShardName(shardName string) StorageOpt {
	return func(storage *Storage) *Storage {
		storage.metrics = storage.metrics.WithTags(map[string]string{"shard_name": shardName})
		storage.logger = log.With(logger.Log, log.String("shard_name", shardName))
		return storage
	}
}

func WithMetrics(registry metrics.Registry) StorageOpt {
	return func(storage *Storage) *Storage {
		storage.metrics = stats.NewSourceStats(registry)
		return storage
	}
}

func WithOpts(storage *Storage, opts ...StorageOpt) *Storage {
	for _, opt := range opts {
		storage = opt(storage)
	}
	return storage
}

func WithHomo() StorageOpt {
	return func(storage *Storage) *Storage {
		storage.IsHomo = true
		return storage
	}
}

func WithTableFilter(f abstract.Includeable) StorageOpt {
	return func(storage *Storage) *Storage {
		storage.tableFilter = f
		return storage
	}
}

func parseSemver(version string) (*semver.Version, error) {
	parts := strings.Split(version, ".")
	if len(parts) < 4 {
		return nil, xerrors.Errorf("unexpected version: %v", version)
	}
	major, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse major: %v: %w", parts[0], err)
	}
	minor, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse minor: %v: %w", parts[1], err)
	}
	patch, _ := strconv.ParseUint(parts[2], 10, 64)
	return &semver.Version{
		Major: major,
		Minor: minor,
		Patch: patch,
	}, nil
}

func NewStorage(config *model.ChStorageParams, transfer *dp_model.Transfer, opts ...StorageOpt) (ClickhouseStorage, error) {
	singleHost := false
	if len(config.ConnectionParams.Shards) > 1 {
		res, err := NewShardedFromUrls(config, transfer, opts...)
		if err != nil {
			return nil, xerrors.Errorf("unable to create sharded storage from urls: %w", err)
		}
		return res, nil
	}

	singleHost = singleHost || topology.IsSingleNode(config.ConnectionParams.Shards) || len(config.ConnectionParams.Hosts) > 1

	db, err := MakeConnection(config)
	if err != nil {
		return nil, xerrors.Errorf("unable to init CH storage: %w", err)
	}
	version, err := backoff.RetryNotifyWithData(func() (string, error) {
		var version string

		if err := db.QueryRow("select version();").Scan(&version); err != nil {
			if errors.IsFatalClickhouseError(err) {
				return "", backoff.Permanent(xerrors.Errorf("unable to select clickhouse version: %w", err))
			}
			return "", xerrors.Errorf("unable to select clickhouse %s version: %w", config.String(), err)
		}
		return version, nil
	}, backoff.NewExponentialBackOff(), util.BackoffLoggerWarn(logger.Log, "version resolver"))
	if err != nil {
		return nil, xerrors.Errorf("unable to extract version: %w", err)
	}

	parsedVersion, err := parseSemver(version)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse semver: %w", err)
	}
	return WithOpts(&Storage{
		db:        db,
		database:  config.ConnectionParams.Database,
		cluster:   config.ChClusterName,
		bufSize:   config.BufferSize,
		logger:    logger.Log,
		onceClose: sync.Once{},
		IsHomo:    false,
		metrics:   stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		version:   *parsedVersion,

		tableFilter:           nil,
		allowSingleHostTables: singleHost,
	}, opts...), nil
}
