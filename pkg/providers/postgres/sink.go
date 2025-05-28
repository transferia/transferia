package postgres

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	listUniqueIndexKeys = `
WITH sample_unique_indexes AS (
    SELECT DISTINCT ON(indrelid)
        *
    FROM pg_index
    WHERE indisunique AND indpred IS NULL
    ORDER BY indrelid, indisprimary DESC, indnatts ASC
),
key_column_numbers AS (
    SELECT
        sample_unique_indexes.indrelid AS relid,
        pg_class.relname AS relname,
        pg_class.relnamespace AS namespace,
        UNNEST(sample_unique_indexes.indkey) AS colnum
    FROM pg_class
    JOIN sample_unique_indexes ON pg_class.oid = sample_unique_indexes.indrelid
)
SELECT
    nsp.nspname as schema_name,
    num.relname as table_name,
    att.attname as column_name
FROM key_column_numbers num
JOIN pg_attribute att ON num.relid = att.attrelid AND num.colnum = att.attnum
JOIN pg_namespace nsp ON num.namespace = nsp.oid
WHERE nsp.nspname NOT IN ('system', 'information_schema', 'pg_catalog', 'pg_toast')
ORDER BY num.relname ASC, att.attnum ASC
;`

	transferLSNTrack = fmt.Sprintf(`
		select table_name, schema_name, lsn from %s
		where transfer_id = $1`,
		TableLSN,
	)

	LSNTrackTableDDL = fmt.Sprintf(`
		create table if not exists %s(
			transfer_id text,
			schema_name text,
			table_name text,
			lsn bigint,
			primary key (transfer_id, schema_name, table_name)
		);`,
		TableLSN,
	)
)

type sink struct {
	conn                *pgxpool.Pool
	currentConn         *pgxpool.Conn
	config              PgSinkParams
	logger              log.Logger
	metrics             *stats.SinkerStats
	keys                map[string][]string // table name -> key column names
	existingTableSchema map[abstract.TableID]abstract.TableSchema
	currentTX           pgx.Tx
	currentTXID         uint32
	transferID          string
	lsnTrack            map[abstract.TableID]uint64
	pendingTableCounts  map[abstract.TableID]int
}

func (s *sink) Close() error {
	if s.currentTX != nil {
		if err := s.currentTX.Rollback(context.TODO()); err != nil {
			s.logger.Warn("Failed to rollback the current transaction", log.Error(err))
		}
	}
	if s.currentConn != nil {
		s.currentConn.Release()
	}
	s.conn.Close()
	return nil
}

func getUniqueIndexKeys(ctx context.Context, conn *pgxpool.Pool) (map[string][]string, error) {
	rows, err := conn.Query(ctx, listUniqueIndexKeys)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	defer rows.Close()

	result := map[string][]string{}
	for rows.Next() {
		var schemaName, tableName, column string
		if err = rows.Scan(&schemaName, &tableName, &column); err != nil {
			//nolint:descriptiveerrors
			return nil, err
		}
		pgName := abstract.PgName(schemaName, tableName)
		result[pgName] = append(result[pgName], fmt.Sprintf("\"%v\"", column))
	}
	if rows.Err() != nil {
		//nolint:descriptiveerrors
		return nil, rows.Err()
	}
	return result, nil
}

func prepareOriginalType(col abstract.ColSchema) (abstract.ColSchema, error) {
	res := col.Copy()
	if res.OriginalType == "" || !strings.HasPrefix(res.OriginalType, "pg:") {
		pgType, err := DataToOriginal(res.DataType)
		if err != nil {
			return abstract.ColSchema{}, xerrors.Errorf("failed to convert type %q: %w", col.DataType, err)
		}
		res.OriginalType = "pg:" + pgType
	}
	return *res, nil
}

func prepareOriginalTypes(schema []abstract.ColSchema) error {
	for i, v := range schema {
		col, err := prepareOriginalType(v)
		if err != nil {
			return xerrors.Errorf("failed to convert type %q: %w", v.DataType, err)
		}
		schema[i] = col
	}
	return nil
}

// CreateSchemaQuery returns a CREATE SCHEMA IF NOT EXISTS query for the given schema
func createSchemaQuery(schemaName string) string {
	return fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%v"; `, schemaName)
}

// CreateSchemaQueryOptional returns a query to create schema if it is absent. May return empty string, in which case schema must not be created.
// Full table name may contain quotes in schema name, which are removed.
func CreateSchemaQueryOptional(fullTableName string) string {
	parts := strings.Split(fullTableName, ".")
	if len(parts) == 1 {
		return ""
	}
	clearSchemaName := strings.ReplaceAll(parts[0], "\"", "")
	if clearSchemaName != "" && clearSchemaName != "public" {
		return createSchemaQuery(clearSchemaName)
	}
	return ""
}

func (s *sink) checkTable(ctx context.Context, in abstract.TableSchema) error {
	if s.config.GetIsSchemaMigrationDisabled() || !s.config.MaintainTables() {
		return nil
	}
	fqtn := in.TableID().Fqtn()
	if csq := CreateSchemaQueryOptional(fqtn); len(csq) > 0 {
		if _, err := s.conn.Exec(ctx, csq); err != nil {
			s.logger.Warn("Failed to execute CREATE SCHEMA.", log.Error(err))
		}
	}
	if err := s.ensureTableSchema(ctx, in); err != nil {
		return xerrors.Errorf("failed to ensure table schema %s exists or not: %w", in.TableID().Fqtn(), err)
	}
	existingSchema := s.existingTableSchema[in.TableID()]
	if existingSchema.Equal(&in) {
		return nil
	}

	if err := s.CreateTable(ctx, in); err != nil {
		return xerrors.Errorf("create table failed with: %w", err)
	}
	if err := s.CreateEnums(ctx, in); err != nil {
		return xerrors.Errorf("create enums failed with: %w", err)
	}
	if err := s.AlterEnums(ctx, in); err != nil {
		return xerrors.Errorf("alter enums failed with: %w", err)
	}
	if err := s.AlterTable(ctx, in); err != nil {
		return xerrors.Errorf("alter table failed with: %w", err)
	}

	return nil
}

func (s *sink) ensureTableSchema(ctx context.Context, in abstract.TableSchema) error {
	if currSchema, ok := s.existingTableSchema[in.TableID()]; !ok || !currSchema.Equal(&in) {
		conn, err := s.conn.Acquire(ctx)
		if err != nil {
			return xerrors.Errorf("failed to acquire connection from pool: %w", err)
		}
		defer conn.Release()
		tID := in.TableID()
		schema, err := NewSchemaExtractor().LoadSchema(ctx, conn.Conn(), &tID)
		if err != nil {
			return xerrors.Errorf("failed to get current table scheme: %w", err)
		}
		if val, ok := schema[in.TableID()]; ok {
			s.existingTableSchema[in.TableID()] = *val
		}
	}
	return nil
}

func (s *sink) CreateTable(ctx context.Context, in abstract.TableSchema) error {
	if _, ok := s.existingTableSchema[in.TableID()]; ok {
		return nil
	}
	if ctq, err := CreateTableQuery(in.TableID().Fqtn(), in.Columns()); err != nil {
		return xerrors.Errorf("failed to create a SQL query to ensure table existence: %w", err)
	} else {
		if _, err = s.conn.Exec(ctx, ctq); err != nil {
			s.logger.Warn("Failed to execute CREATE TABLE.", log.Error(err))
		}
		s.logger.Info(fmt.Sprintf("%s table created", in.TableID().Fqtn()), log.Any("ddl", ctq))
	}
	return nil
}

func (s *sink) CreateEnums(ctx context.Context, in abstract.TableSchema) error {
	ets := s.existingTableSchema[in.TableID()]
	for _, col := range in.Columns() {
		if !isPgEnum(col) {
			continue
		}
		// if column exists in table type is considered created
		if _, ok := ets.FastColumns()[abstract.ColumnName(col.ColumnName)]; ok {
			continue
		}
		query, err := createEnumQuery(col)
		if err != nil {
			s.logger.Warn("failed to create a SQL query to ensure ENUM existence", log.Error(err))
		}
		if _, err = s.conn.Exec(ctx, query); err != nil {
			s.logger.Warn("Failed to execute CREATE TYPE.", log.Error(err))
		}

		s.logger.Info(fmt.Sprintf("%s type created", in.TableID().Fqtn()), log.Any("ddl", query))
	}
	return nil
}

func isPgEnum(col abstract.ColSchema) bool {
	vals := GetPropertyEnumAllValues(&col)
	return vals != nil
}

func (s *sink) AlterEnums(ctx context.Context, in abstract.TableSchema) error {
	ets := s.existingTableSchema[in.TableID()]
	for _, col := range in.Columns() {
		if !isPgEnum(col) {
			continue
		}
		queries, err := addEnumValsQuery(ets.FastColumns()[abstract.ColumnName(col.ColumnName)], col)
		if err != nil {
			s.logger.Warn("failed to create a SQL query to ensure ENUM values", log.Error(err))
		}
		if len(queries) == 0 {
			continue
		}
		for _, query := range queries {
			if _, err = s.conn.Exec(ctx, query); err != nil {
				s.logger.Warn("Failed to execute ALTER TYPE.", log.Error(err))
			}
		}
		s.logger.Info(fmt.Sprintf("%s type altered", in.TableID().Fqtn()), log.Any("ddl", queries))
	}
	return nil
}

func (s *sink) AlterTable(ctx context.Context, in abstract.TableSchema) error {
	added := make([]abstract.ColSchema, 0, len(in.Columns()))
	ets := s.existingTableSchema[in.TableID()]
	for _, col := range in.Columns() {
		if _, ok := ets.FastColumns()[abstract.ColumnName(col.ColumnName)]; !ok {
			added = append(added, col)
			continue
		}
	}
	if len(added) == 0 {
		return nil
	}
	query, err := addColsQuery(in.TableID().Fqtn(), added)
	if err != nil {
		return xerrors.Errorf("failed to create a SQL query to ensure columns existence: %w", err)
	}
	if _, err = s.conn.Exec(ctx, query); err != nil {
		s.logger.Warn("Failed to execute ALTER TABLE.", log.Error(err))
	}
	s.logger.Info(fmt.Sprintf("%s table altered", in.TableID().Fqtn()), log.Any("ddl", query))
	s.existingTableSchema[in.TableID()] = in
	return nil
}

func DataToOriginal(dataType string) (string, error) {
	switch strings.ToLower(dataType) {
	case "int8", "uint8", "int16", "uint16":
		return "smallint", nil
	case "int32", "uint32":
		return "integer", nil
	case "int64", "uint64":
		return "bigint", nil
	case "float32", "float":
		return "real", nil
	case "float64", "double":
		return "double precision", nil
	case "bool", "boolean":
		return "boolean", nil
	case "utf8":
		return "text", nil
	case "string":
		return "bytea", nil
	case "date":
		return "date", nil
	case "datetime", "timestamp":
		return "timestamp without time zone", nil
	case "any":
		return "jsonb", nil
	case "interval":
		return "text", nil
	default:
		return "", xerrors.Errorf("Can't process type '%v'", dataType)
	}
}

type txBatch struct {
	txID           uint32
	lsn            uint64
	queries        []string
	size           int
	tableRowCounts map[abstract.TableID]int
}

func (t *txBatch) txQuery(transferID string) string {
	res := strings.Join(t.queries, ";\n")
	for table := range t.tableRowCounts {
		res += fmt.Sprintf(`
			insert into %s (transfer_id, table_name, schema_name, lsn) values ('%v', '%v', '%v', %v)
			on conflict (transfer_id, schema_name, table_name) do update set lsn = excluded.lsn;`,
			TableLSN, transferID, table.Name, table.Namespace, t.lsn,
		)
	}
	return res
}

func (s *sink) runTX(batch txBatch) error {
	if s.currentTXID != batch.txID && s.currentTX != nil {
		if err := s.currentTX.Commit(context.TODO()); err != nil {
			return xerrors.Errorf("unable to commit transaction %v: %w", s.currentTXID, err)
		}
		s.currentTXID = batch.txID
		s.currentTX = nil
		var txRes []string
		for t, c := range s.pendingTableCounts {
			s.metrics.Table(t.Fqtn(), "rows", c)
			txRes = append(txRes, fmt.Sprintf("%v.%v(%v rows)", t.Namespace, t.Name, c))
		}
		s.logger.Infof("committed tx: %v, table set: %v", s.currentTXID, strings.Join(txRes, ","))
		s.pendingTableCounts = map[abstract.TableID]int{}
	}
	var err error
	if s.currentTX == nil {
		s.currentTX, err = s.currentConn.Begin(context.TODO())
		if err != nil {
			return xerrors.Errorf("unable to begin transaction: %w", err)
		}
		s.currentTXID = batch.txID
	}
	txQuery := batch.txQuery(s.transferID)
	s.logger.Infof("execute tx%v query: \n%v", batch.txID, util.Sample(txQuery, 1000))
	_, err = s.currentTX.Exec(context.TODO(), txQuery)
	if err != nil {
		return xerrors.Errorf("unable exec query for tx: %v: %w", batch.txID, err)
	}
	for t, rowCount := range batch.tableRowCounts {
		s.pendingTableCounts[t] += rowCount
	}
	return nil
}

func (s *sink) Push(input []abstract.ChangeItem) error {
	if s.config.PerTransactionPush() {
		//nolint:descriptiveerrors
		return s.perTransactionPush(input)
	} else {
		//nolint:descriptiveerrors
		return s.batchInsert(input)
	}
}

func (s *sink) perTransactionPush(input []abstract.ChangeItem) error {
	if s.currentConn == nil {
		conn, err := s.conn.Acquire(context.TODO())
		if err != nil {
			return xerrors.Errorf("unable to acquire connection from poll: %w", err)
		}
		s.currentConn = conn
	}
	var err error
	s.logger.Infof("Prepare batch %v", len(input))
	var txs []txBatch
	for _, tx := range abstract.SplitByID(input) {
		batch := input[tx.Left:tx.Right]
		size := 0
		tableRowCounts := map[abstract.TableID]int{}
		queries := make([]string, len(batch))
		for i, r := range batch {
			if r.LSN > 0 && r.ID > 0 {
				if savedLSN, ok := s.lsnTrack[r.TableID()]; ok && savedLSN >= r.LSN {
					s.logger.Info("Found an item with LSN less than the one in destination, skip it as save_tx_boundaries is enabled", log.UInt32("txid", r.ID), log.UInt64("lsn", r.LSN), log.String("table", r.PgName()), log.UInt64("saved_lsn", savedLSN))
					continue
				}
			} else {
				// consider this the same as PostgreSQL frozen XID
				s.logger.Info("Found an item with zero LSN or zero transaction ID while save_tx_boundaries is enabled", log.String("kind", string(r.Kind)))
			}
			if !r.IsRowEvent() {
				s.logger.Info("Pushing non-row item outside of transaction with save_tx_boundaries enabled", log.String("kind", string(r.Kind)))
				if err := s.batchInsert(batch[i : i+1]); err != nil {
					return xerrors.Errorf("failed to execute non-row item push outside of transaction with save_tx_boundaries enabled: %w", err)
				}
				continue
			}
			s.metrics.Table(r.PgName(), "rows", 1)
			queries[i], err = s.buildQuery(r.PgName(), r.TableSchema.Columns(), batch[i:i+1])
			if err != nil {
				return xerrors.Errorf("unable to build query: %w", err)
			}
			size += len(queries[i])
			tableRowCounts[r.TableID()]++
		}
		if size > 0 {
			txs = append(txs, txBatch{
				txID:           batch[0].ID,
				lsn:            batch[len(batch)-1].LSN,
				size:           size,
				queries:        queries,
				tableRowCounts: tableRowCounts,
			})
		}
	}
	if len(txs) == 0 {
		return nil
	}
	if len(txs) < 3 {
		// we have 1 or 2 transaction, no need to squash in between transactions
		for _, tx := range txs {
			if err := s.runTX(tx); err != nil {
				//nolint:descriptiveerrors
				return err
			}
		}
	} else {
		// we have at least 3 tx, there for
		// every tx except last one can be squashed into one large tx
		// last tx will just closed squashed in between txs and open new one, but not committed it
		var inBetweenTX txBatch
		inBetweenTX.queries = []string{}
		inBetweenTX.tableRowCounts = map[abstract.TableID]int{}

		var squashedTXs []uint32
		for _, tx := range txs[0 : len(txs)-1] {
			squashedTXs = append(squashedTXs, tx.txID)
			inBetweenTX.queries = append(inBetweenTX.queries, tx.queries...)
			inBetweenTX.txID = tx.txID
			inBetweenTX.lsn = tx.lsn
			for table, count := range tx.tableRowCounts {
				inBetweenTX.tableRowCounts[table] += count
			}
		}
		s.logger.Infof("squashed %v into %v", squashedTXs, inBetweenTX.txID)
		// run squashed in between txs
		if err := s.runTX(inBetweenTX); err != nil {
			//nolint:descriptiveerrors
			return err
		}

		// begin last batch tx
		if err := s.runTX(txs[len(txs)-1]); err != nil {
			//nolint:descriptiveerrors
			return err
		}
	}
	return nil
}

func (s *sink) batchInsert(input []abstract.ChangeItem) error {
	batches := make(map[string][]abstract.ChangeItem)
	for _, item := range input {
		switch item.Kind {
		case abstract.PgDDLKind:
			if _, err := s.conn.Exec(context.TODO(), item.ColumnValues[0].(string)); err != nil {
				s.logger.Warnf("Unable to exec:\n%v", item.ColumnValues[0].(string))
				//nolint:descriptiveerrors
				return err
			}
			newKeys, err := getUniqueIndexKeys(context.TODO(), s.conn)
			if err != nil {
				//nolint:descriptiveerrors
				return err
			}
			s.keys = newKeys
		case abstract.DropTableKind:
			if s.config.CleanupMode() != model.Drop {
				s.logger.Infof("Skipped dropping table '%v' due cleanup policy", item.PgName())
				continue
			}
			if _, err := s.conn.Exec(context.TODO(), fmt.Sprintf("drop table if exists %v;", item.PgName())); err != nil {
				if IsPgError(err, ErrcWrongObjectType) {
					s.logger.Infof("drop table %v returns: %v, so try drop as view", item.PgName(), err.Error())
					if _, err := s.conn.Exec(context.TODO(), fmt.Sprintf("drop view if exists %v;", item.PgName())); err != nil {
						//nolint:descriptiveerrors
						return err
					}
				} else {
					//nolint:descriptiveerrors
					return err
				}
			}
		case abstract.TruncateTableKind:
			if s.config.CleanupMode() != model.Truncate {
				s.logger.Infof("Skipped truncating table '%v' due cleanup policy", item.PgName())
				continue
			}
			if _, err := s.conn.Exec(context.TODO(), fmt.Sprintf("truncate table %v;", item.PgName())); err != nil {
				if IsPgError(err, ErrcWrongObjectType) {
					s.logger.Infof("truncate table %v returns: %v, so this is view, no need to truncate", item.PgName(), err.Error())
					continue
				} else if IsPgError(err, ErrcRelationDoesNotExists) {
					s.logger.Infof("truncate table %v skip: table not exists", item.PgName())
					continue
				} else if IsPgError(err, ErrcSchemaDoesNotExists) {
					s.logger.Infof("truncate table %v skip: schema %v not exists", item.PgName(), item.Schema)
					continue
				} else {
					//nolint:descriptiveerrors
					return err
				}
			}
		case abstract.InitShardedTableLoad, abstract.DoneShardedTableLoad, abstract.SynchronizeKind:
			// not needed for now
		case abstract.InitTableLoad, abstract.DoneTableLoad:
			if s.config.PerTransactionPush() {
				_, err := s.conn.Exec(context.TODO(), fmt.Sprintf(`
					insert into %s (transfer_id, table_name, schema_name, lsn) values ($1, $2, $3, $4)
					on conflict (transfer_id, schema_name, table_name) do update set lsn = excluded.lsn;`,
					TableLSN,
				), s.transferID, item.Table, item.Schema, item.LSN)
				if err != nil {
					//nolint:descriptiveerrors
					return err
				}
			}
		case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind:
			batches[item.PgName()] = append(batches[item.PgName()], item)
		default:
			s.logger.Infof("kind: %v not supported", item.Kind)
		}
	}
	for table, batch := range batches {
		pgTable := table
		if s.config.Tables()[table] != "" {
			pgTable = s.config.Tables()[table]
		}
		tableID, err := abstract.NewTableIDFromStringPg(pgTable, false)
		if err != nil {
			return xerrors.Errorf("failed to parse TableID from %s: %w", pgTable, err)
		}
		batch[0].TableSchema.SetTableID(*tableID)
		if err := s.checkTable(context.TODO(), *batch[0].TableSchema); err != nil {
			//nolint:descriptiveerrors
			return err
		}
		tableSchema := batch[0].TableSchema.Columns()
		// TODO: This must be moved into middleware (and the same must be done in other sinks)
		batch = abstract.Collapse(batch)

		if s.config.CopyUpload() {
			copyCtx, copyCtxCancel := context.WithTimeout(context.Background(), s.config.QueryTimeout())
			defer copyCtxCancel()
			if err := s.copy(copyCtx, batch); err != nil {
				if s.config.DisableSQLFallback() {
					return abstract.NewFatalError(xerrors.Errorf("COPY FROM failed: %w; SQL fallback is disabled", err))
				}
				s.logger.Warn("Batch insert with COPY failed. Will retry insert using common INSERT", log.Error(err))
			} else {
				s.metrics.Table(table, "rows", len(batch))
				continue
			}
		}
		insertCtx, insertCtxCancel := context.WithTimeout(context.Background(), s.config.QueryTimeout())
		defer insertCtxCancel()
		if err := s.insert(insertCtx, pgTable, tableSchema, batch); err != nil {
			s.metrics.Table(table, "error", 1)
			return xerrors.Errorf("failed to insert %d rows into table %s using plain INSERT: %w", len(batch), table, err)
		}
		s.metrics.Table(table, "rows", len(batch))
	}

	return nil
}

func Represent(val interface{}, colSchema abstract.ColSchema) (string, error) {
	if val == nil {
		return "null", nil
	}

	if v, ok := val.(*pgtype.CIDR); ok {
		// pgtype.CIDR does not implement driver.Valuer, but its implementation is intended
		val = (*pgtype.Inet)(v)
	}

	if v, ok := val.(driver.Valuer); ok {
		vv, _ := v.Value()

		if strings.HasPrefix(colSchema.OriginalType, "pg:time") &&
			!strings.HasPrefix(colSchema.OriginalType, "pg:timestamp") {
			// by default Value of time always returns as array of bytes which can not be processed in plain insert
			// however if we cast decoder to pgtype.Time while unmarshalling it will lead to errors in tests because
			// pgtype.Time doesn't store the precision and always uses the maximum(6)
			if vvv, ok := vv.([]byte); ok && vv != nil {
				coder := new(pgtype.Time)

				// we only use binary->binary (de)serialization in homogeneous pg->pg
				if err := coder.DecodeBinary(nil, vvv); err == nil {
					//nolint:descriptiveerrors
					return Represent(coder, colSchema)
				}
			}
		}

		if strings.HasPrefix(colSchema.OriginalType, "pg:json") {
			if vvv, ok := vv.(string); ok && vv != nil {
				// Valuer may start with special character, which we must erase
				// If JSON not started with { or ] we should erase first byte
				if vvv[0] != '{' && vvv[0] != '[' {
					result, err := Represent(vvv[1:], colSchema)
					if err != nil {
						return "", xerrors.Errorf("unable to represent value from pg:json/pg:jsonb: %w", err)
					}
					return result, nil
				}
			}
			if vvv, ok := vv.([]uint8); ok && vv != nil {
				escapedQuotesStr := escapeSingleQuotes(string(vvv))
				return strings.Join([]string{"'", escapedQuotesStr, "'"}, ""), nil
			}
		}
		//nolint:descriptiveerrors
		return Represent(vv, colSchema)
	}

	if colSchema.OriginalType == "" && colSchema.DataType == schema.TypeAny.String() { // no-homo json
		s, _ := json.Marshal(val)
		return fmt.Sprintf("'%s'", escapeSingleQuotes(string(s))), nil
	}
	if colSchema.OriginalType == "pg:json" || colSchema.OriginalType == "pg:jsonb" {
		s, _ := json.Marshal(val)
		v := escapeSingleQuotes(string(s))
		return fmt.Sprintf("'%v'", v), nil
	}

	// handle pg:enum in homo cases, when it was fallback from COPY (if CopyUpload() allowed)
	if strings.HasPrefix(colSchema.OriginalType, "pg:") && colSchema.DataType == schema.TypeAny.String() && GetPropertyEnumAllValues(&colSchema) != nil {
		if bytes, ok := val.([]byte); ok {
			return "'" + strings.ReplaceAll(string(bytes), "'", "''") + "'", nil
		}
	}

	switch v := val.(type) {
	case string:
		switch colSchema.DataType {
		case schema.TypeBytes.String():
			return fmt.Sprintf("'%s'", escapeBackslashes(escapeSingleQuotes(v))), nil
		default:
			return fmt.Sprintf("'%s'", escapeSingleQuotes(v)), nil
		}
	case *time.Time:
		return representTime(*v, colSchema), nil
	case time.Time:
		return representTime(v, colSchema), nil
	case []byte:
		return fmt.Sprintf("'\\x%s'", hex.EncodeToString(v)), nil
	case int64, int, int32, uint32, uint64:
		return fmt.Sprintf("'%d'", v), nil
	case float64:
		if strings.HasPrefix(colSchema.DataType, "uint") {
			return fmt.Sprintf("'%d'", uint64(v)), nil
		}
		if strings.HasPrefix(colSchema.DataType, "int") || colSchema.OriginalType == "pg:bigint" {
			return fmt.Sprintf("'%d'", int64(v)), nil
		}
		if strings.Contains(colSchema.OriginalType, "pg:") && strings.Contains(colSchema.OriginalType, "int") {
			return fmt.Sprintf("'%d'", int64(v)), nil
		}
		if colSchema.DataType == schema.TypeFloat64.String() {
			// Will print all avaible float point numbers
			return fmt.Sprintf("'%g'", v), nil
		}
		return fmt.Sprintf("'%f'", v), nil
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, value := range v {
			elemColSchema := BuildColSchemaArrayElement(colSchema)
			representedVal, err := Represent(value, elemColSchema)
			if err != nil {
				return "", xerrors.Errorf("unable to represent array element: %w", err)
			}
			result = append(result, UnwrapArrayElement(representedVal))
		}
		lBracket, rBracket := workaroundChooseBrackets(colSchema)
		return "'" + lBracket + strings.Join(result, ",") + rBracket + "'", nil
	default:
		if colSchema.OriginalType == "pg:hstore" {
			s, _ := json.Marshal(val)
			h, _ := JSONToHstore(string(s))
			v := escapeSingleQuotes(h)
			return fmt.Sprintf("'%v'", v), nil
		}
		return fmt.Sprintf("'%v'", v), nil
	}
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func escapeBackslashes(s string) string {
	return strings.ReplaceAll(s, "\\", "\\\\")
}

func workaroundChooseBrackets(colSchema abstract.ColSchema) (string, string) {
	if colSchema.OriginalType == "pg:json" || colSchema.OriginalType == "pg:jsonb" {
		return "[", "]"
	}
	return "{", "}"
}

const (
	PgDateFormat      = "2006-01-02"
	PgDatetimeFormat  = "2006-01-02 15:04:05Z07:00:00"
	PgTimestampFormat = "2006-01-02 15:04:05.999999Z07:00:00"
)

func representTime(v time.Time, colSchema abstract.ColSchema) string {
	var result string

	switch colSchema.DataType {
	case schema.TypeDate.String():
		result = v.UTC().Format(PgDateFormat)
	case schema.TypeDatetime.String():
		result = v.UTC().Format(PgDatetimeFormat)
	case schema.TypeTimestamp.String():
		// note `v` is not converted to UTC. As a result, when the target field is TIMESTAMP WITHOUT TIME ZONE, the incoming value of the timestamp will be preserved, but its timezone will be (automatically) set to the local time zone of the target database
		result = v.Format(PgTimestampFormat)
	default:
		result = v.UTC().Format(PgTimestampFormat)
	}

	result = MinusToBC(result)

	return "'" + result + "'"
}

func RepresentWithCast(v interface{}, colSchema abstract.ColSchema) (string, error) {
	representation, err := Represent(v, colSchema)
	if err != nil {
		return "", xerrors.Errorf("failed to represent %v in the form suitable for PostgreSQL sink: %w", v, err)
	}

	if strings.HasPrefix(colSchema.OriginalType, "pg:") && !IsUserDefinedType(&colSchema) {
		castTarget := strings.TrimPrefix(colSchema.OriginalType, "pg:")
		representation = strings.Join([]string{representation, castTarget}, "::")
	}

	return representation, nil
}

func isToastedRow(row *abstract.ChangeItem, schema []abstract.ColSchema) bool {
	return len(row.ColumnNames) != len(schema)
}

func (s *sink) isTableWithKeys(table string) bool {
	return len(s.keys[table]) > 0
}

// tryGetUnchangedCols get unchanged columns if applicable, look into OldKeys and compare them with Values
// this applicable only if table has primary key and Replica Identity Full enabled.
func (s *sink) tryGetUnchangedCols(
	generatedCols map[string]bool,
	rev map[string]int,
	schema []abstract.ColSchema,
	row abstract.ChangeItem,
) (map[string]bool, error) {
	oldKeysIDXs := s.getOldKeyIDXs(row)
	if !s.config.PerTransactionPush() {
		// we should push all columns if no transaction push
		return nil, nil
	}

	res := map[string]bool{}
	for i, colName := range row.ColumnNames {
		// rev must exist now
		if generatedCols[colName] {
			continue
		}
		thisCS := schema[rev[colName]]
		newVal, err := Represent(row.ColumnValues[i], thisCS)
		if err != nil {
			return nil, xerrors.Errorf("Cannot represent value of column %s: %w", colName, err)
		}
		if oldIDX, ok := oldKeysIDXs[colName]; ok {
			schemaIndex, ok := rev[colName]
			if !ok {
				return nil, xerrors.Errorf("Key \"%v\" not found in source table schema", colName)
			}
			oldVal, err := Represent(row.OldKeys.KeyValues[oldIDX], schema[schemaIndex])
			if err != nil {
				return nil, xerrors.Errorf("Cannot represent key %s: %w", colName, err)
			}
			if oldVal == newVal {
				res[colName] = true
			}
		}
	}
	return res, nil
}

func (s *sink) getGeneratedCols(schema []abstract.ColSchema) map[string]bool {
	genColumnsSet := make(map[string]bool)
	for _, cs := range schema {
		if cs.Expression != "" {
			genColumnsSet[cs.ColumnName] = true
		}
	}
	return genColumnsSet
}

func (s *sink) getOldKeyIDXs(row abstract.ChangeItem) map[string]int {
	oldKeysIDXs := map[string]int{}
	for i, col := range row.OldKeys.KeyNames {
		oldKeysIDXs[col] = i
	}
	return oldKeysIDXs
}

func (s *sink) buildInsertQuery(
	table string,
	schema []abstract.ColSchema,
	row abstract.ChangeItem,
	rev map[string]int,
) (string, error) {
	// generated columns
	generatedCols := s.getGeneratedCols(schema)
	unchangedCols, err := s.tryGetUnchangedCols(generatedCols, rev, schema, row)
	if err != nil {
		return "", xerrors.Errorf("unable to get unchanged values: %w", err)
	}

	totalColNumber := len(row.ColumnNames)
	generatedColNumber := len(generatedCols)
	unchangedColNumber := len(unchangedCols)
	colsToUpdate := totalColNumber - generatedColNumber - unchangedColNumber

	// This may happen if postgresql update doesn't actually change anything (it is still present in WAL though) and per transaction push is enabled
	if colsToUpdate == 0 {
		return "", nil
	} else if colsToUpdate < 0 {
		return "", xerrors.Errorf("unexpected negative number of columns to update for table %s, got %d total columns, %d is generated and %d unchanged",
			table, totalColNumber, generatedColNumber, unchangedColNumber)
	}
	values := make([]string, colsToUpdate)
	colNames := make([]string, colsToUpdate)
	iEC := 0
	for i, colName := range row.ColumnNames {
		// rev must exist now
		if generatedCols[colName] {
			continue
		}
		if unchangedCols != nil && unchangedCols[colName] {
			continue
		}
		thisCS := schema[rev[colName]]

		representation, err := RepresentWithCast(row.ColumnValues[i], thisCS)
		if err != nil {
			return "", xerrors.Errorf("failed to represent the new value of column %q: %w", colName, err)
		}
		values[iEC] = representation
		colNames[iEC] = fmt.Sprintf("\"%v\"", colName)
		iEC += 1
	}

	if row.Kind == abstract.UpdateKind && (row.KeysChanged() || isToastedRow(&row, schema) || s.config.PerTransactionPush() || !s.isTableWithKeys(table)) {
		setters := make([]string, 0)
		for iEC := range colNames {
			setters = append(setters, fmt.Sprintf("%v = %v", colNames[iEC], values[iEC]))
		}
		predicate := make([]string, 0)
		for i, keyName := range row.OldKeys.KeyNames {
			if i >= len(row.OldKeys.KeyValues) {
				return "", xerrors.Errorf("No value found for old key named %s", keyName)
			}
			schemaIndex, ok := rev[keyName]
			if !ok {
				return "", xerrors.Errorf("Key \"%v\" not found in source table schema", keyName)
			}
			keyValue, err := RepresentWithCast(row.OldKeys.KeyValues[i], schema[schemaIndex])
			if err != nil {
				return "", xerrors.Errorf("failed to represent the old value of a key column %q: %w", keyName, err)
			}
			predicate = append(predicate, fmt.Sprintf("\"%v\" = %v", keyName, keyValue))
		}
		if len(predicate) > 0 && len(setters) > 0 {
			return fmt.Sprintf(
				"update %v set %v where %v;",
				table,
				strings.Join(setters, ", "),
				strings.Join(predicate, " and "),
			), nil
		}
	}
	insertQuery := fmt.Sprintf(
		"insert into %v (%v) values (%v)",
		table,
		strings.Join(colNames, ", "),
		strings.Join(values, ", "))
	if keyCols, ok := s.keys[table]; ok && len(keyCols) > 0 {
		excludedNames := make([]string, len(colNames))
		for i := range colNames {
			excludedNames[i] = "excluded." + colNames[i]
		}
		insertQuery += fmt.Sprintf(
			" on conflict (%v) do update set (%v)=row(%v)",
			strings.Join(keyCols, ", "),
			strings.Join(colNames, ", "),
			strings.Join(excludedNames, ", "))
	}
	return insertQuery + ";", nil
}

func (s *sink) buildDeleteQuery(table string, schema []abstract.ColSchema, row abstract.ChangeItem, rev map[string]int) (string, error) {
	deleteConditions := make([]string, len(row.OldKeys.KeyNames))
	for idx := range row.OldKeys.KeyNames {
		var keyType, keyOrigType string
		columnIndex := rev[row.OldKeys.KeyNames[idx]]
		keyType = schema[columnIndex].DataType
		keyOrigType = schema[columnIndex].OriginalType

		repr, err := RepresentWithCast(row.OldKeys.KeyValues[idx], abstract.MakeOriginallyTypedColSchema(row.OldKeys.KeyNames[idx], keyType, keyOrigType))
		if err != nil {
			return "", xerrors.Errorf("failed to represent the old value of a key column %q: %w", row.OldKeys.KeyNames[idx], err)
		}
		deleteConditions[idx] = fmt.Sprintf(
			"(\"%s\" = %s)",
			row.OldKeys.KeyNames[idx],
			repr,
		)
	}
	if len(deleteConditions) == 0 {
		s.logger.Errorf("no where clause for delet statement in change item:\n%s", row.ToJSONString())
		return "", abstract.NewFatalError(xerrors.New("Unable to build DELETE query, no key names presented"))
	}
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %v", table, strings.Join(deleteConditions, " AND "))
	return deleteQuery + ";", nil
}

// buildQueries constructs a list of SQL queries to push the given items into PostgreSQL.
// Each query in non-nil output strictly corresponds to an item in the given items
func (s *sink) buildQueries(table string, schema []abstract.ColSchema, items []abstract.ChangeItem) ([]string, error) {
	rev := abstract.MakeMapColNameToIndex(schema)

	result := make([]string, 0, len(items))

	for _, row := range items {
		if row.Kind == abstract.DeleteKind {
			deleteQuery, err := s.buildDeleteQuery(table, schema, row, rev)
			if err != nil {
				return nil, xerrors.Errorf("failed to build DELETE query: %w", err)
			}
			result = append(result, deleteQuery)
		} else {
			insertQuery, err := s.buildInsertQuery(table, schema, row, rev)
			if err != nil {
				return nil, xerrors.Errorf("failed to build INSERT query: %w", err)
			}
			result = append(result, insertQuery)
		}
	}

	return result, nil
}

// buildQuery is a wrapper around buildQueries which makes sure the output contains exactly one query
func (s *sink) buildQuery(table string, schema []abstract.ColSchema, items []abstract.ChangeItem) (string, error) {
	queries, err := s.buildQueries(table, schema, items)
	if err != nil {
		return "", err
	}
	if len(queries) > 1 {
		return "", abstract.NewFatalError(xerrors.Errorf("a call to buildQuery returned %d items instead of one", len(items)))
	}
	return queries[0], nil
}

// executeQueries executes the given queries using the given connection.
func (s *sink) executeQueries(ctx context.Context, conn *pgx.Conn, queries []string) error {
	combinedQuery := strings.Join(queries, "\n")
	_, err := conn.Exec(ctx, combinedQuery, pgx.QuerySimpleProtocol(true))
	if err == nil {
		return nil
	}

	if s.config.LoozeMode() {
		s.logger.Error("Failed to execute queries at sink. Skipping an error as the looze mode is on", log.Error(err))
		return nil
	}

	s.logger.Warn("failed to execute queries at sink", log.String("query", util.DefaultSample(combinedQuery)), log.Error(err), log.Int("len", len(queries)))
	return xerrors.Errorf("failed to execute %d queries at sink: %w", len(queries), err)
}

func (s *sink) insert(ctx context.Context, table string, schema []abstract.ColSchema, items []abstract.ChangeItem) error {
	if err := prepareOriginalTypes(schema); err != nil {
		return err
	}

	conn, err := s.conn.Acquire(ctx)
	if err != nil {
		return xerrors.Errorf("failed to acquire a connection at sink: %w", err)
	}
	defer conn.Release()

	queries, err := s.buildQueries(table, schema, items)
	if err != nil {
		return xerrors.Errorf("failed to build queries to process items at sink: %w", err)
	}

	// s.logger.Infof("Prepare query %v rows %v for table %v", len(items), format.SizeInt(len(queries)), table)
	execStart := time.Now()
	processedQueries := 0
	for processedQueries < len(queries) {
		queriesBatchSizeInBytes := uint64(0)
		batchFinishI := processedQueries
		// Limit queries' size by 64 MiB
		// https://dba.stackexchange.com/questions/131399/is-there-a-maximum-length-constraint-for-a-postgres-query
		for batchFinishI < len(queries) && queriesBatchSizeInBytes < uint64(64*humanize.MiByte) {
			queriesBatchSizeInBytes += uint64(len(queries[batchFinishI]))
			batchFinishI += 1
		}

		err := s.executeQueries(ctx, conn.Conn(), queries[processedQueries:batchFinishI])
		if IsPgError(err, ErrcUniqueViolation) && s.config.IgnoreUniqueConstraint() {
			// This may happen when the state of the target database is newer than the WAL entries
			// we are currently reading. We can safely ignore such errors (I hope) because, assuming
			// the source DB has the same unique constraints, eventually we will read the log up until
			// the point where the conflicting entry is created and upsert it into the target anyway.
			// See TM-1027
			s.logger.Warn("UNIQUE constraint violation while executing a batch of queries. Retrying queries one-by-one, as ignore UNIQUE constraint option is enabled", log.Error(err))
			for i := processedQueries; i < batchFinishI; i++ {
				err := s.executeQueries(ctx, conn.Conn(), []string{queries[i]})
				if err == nil {
					continue
				}
				if IsPgError(err, ErrcUniqueViolation) {
					if !items[i].KeysChanged() {
						s.logger.Warn("ignoring UNIQUE constraint violation and skipping an item", log.String("table", table), log.String("query", util.DefaultSample(queries[i])), log.Error(err))
						s.metrics.Table(table, "ignored_error", 1)
						continue
					}
					s.logger.Error("cannot ignore UNIQUE constraint violation because PRIMARY KEY columns are changed by an item", log.Any("query", util.DefaultSample(queries[i])), log.Error(err))
				}
				s.logger.Warn("single item query failed at sink", log.Any("query", util.DefaultSample(queries[i])), log.Error(err))
				return xerrors.Errorf("failed to process a single item at sink: %w", err)
			}
			err = nil
		}
		if err != nil {
			return xerrors.Errorf("failed to process items at sink: %w", err)
		}

		processedQueries = batchFinishI
	}

	s.logger.Infof("successfully processed %d queries at sink in %s", len(queries), time.Since(execStart).String())
	return nil
}

type copyBatch struct {
	data []abstract.ChangeItem
	iter int
}

func (b *copyBatch) Values() ([]interface{}, error) {
	return b.data[b.iter].ColumnValues, nil
}

func (b *copyBatch) Err() error {
	return nil
}

func (b *copyBatch) Next() bool {
	b.iter++
	return b.iter < len(b.data)
}

func (s *sink) copy(ctx context.Context, batch []abstract.ChangeItem) error {
	b := &copyBatch{
		data: batch,
		iter: -1,
	}
	_, err := s.conn.CopyFrom(ctx, []string{batch[0].Schema, batch[0].Table}, batch[0].ColumnNames, b)
	return err
}

func getLsnTrack(ctx context.Context, conn *pgxpool.Pool, transferID string) (map[abstract.TableID]uint64, error) {
	q, err := conn.Query(ctx, transferLSNTrack, transferID)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	defer q.Close()
	res := map[abstract.TableID]uint64{}
	for q.Next() {
		var tableID abstract.TableID
		var lsn uint64
		if err := q.Scan(&tableID.Name, &tableID.Namespace, &lsn); err != nil {
			//nolint:descriptiveerrors
			return nil, err
		}
		res[tableID] = lsn
	}
	if q.Err() != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	return res, nil
}

func NewSink(lgr log.Logger, transferID string, config PgSinkParams, mtrcs metrics.Registry) (abstract.Sinker, error) {
	r := util.Rollbacks{}
	defer r.Do()

	connConfig, err := MakeConnConfigFromSink(lgr, config)
	if err != nil {
		return nil, err
	}

	poolConfig, _ := pgxpool.ParseConfig("")
	poolConfig.ConnConfig = WithLogger(connConfig, log.With(lgr, log.Any("component", "pgx")))
	poolConfig.MaxConns = 5
	poolConfig.AfterConnect = MakeInitDataTypes()

	pool, err := NewPgConnPoolConfig(context.TODO(), poolConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to make a connection pool: %w", err)
	}
	r.Add(func() {
		pool.Close()
	})

	result, err := NewSinkWithPool(context.TODO(), lgr, transferID, config, mtrcs, pool)
	if err != nil {
		return nil, xerrors.Errorf("failed to initialize a sink with pool: %w", err)
	}

	r.Cancel()

	return result, nil
}

func NewSinkWithPool(ctx context.Context, lgr log.Logger, transferID string, config PgSinkParams, mtrcs metrics.Registry, pool *pgxpool.Pool) (abstract.Sinker, error) {
	keys, err := getUniqueIndexKeys(ctx, pool)
	if err != nil {
		return nil, xerrors.Errorf("failed to get UNIQUE INDEX keys: %w", err)
	}
	result := &sink{
		conn:                pool,
		currentConn:         nil,
		config:              config,
		logger:              lgr,
		metrics:             stats.NewSinkerStats(mtrcs),
		keys:                keys,
		existingTableSchema: make(map[abstract.TableID]abstract.TableSchema),
		currentTX:           nil,
		currentTXID:         0,
		transferID:          transferID,
		lsnTrack:            map[abstract.TableID]uint64{},
		pendingTableCounts:  map[abstract.TableID]int{},
	}

	if config.PerTransactionPush() {
		if _, err := pool.Exec(ctx, LSNTrackTableDDL); err != nil {
			return nil, xerrors.Errorf("failed to CREATE TABLE to track LSNs: %w", err)
		}
		lsnTrack, err := getLsnTrack(ctx, pool, transferID)
		if err != nil {
			return nil, xerrors.Errorf("failed to get LSN track: %w", err)
		}
		result.lsnTrack = lsnTrack
	}

	return result, nil
}
