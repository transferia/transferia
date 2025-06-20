//go:build !disable_greenplum_provider

package greenplum

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	pgsink "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func temporaryTable(schema string, name string) (ttSchema string, ttName string) {
	return schema, "_dt_" + name
}

func (s *Sink) processInitTableLoad(ctx context.Context, ci *abstract.ChangeItem) error {
	strg, err := s.sinks.PGStorage(ctx, Coordinator())
	if err != nil {
		return xerrors.Errorf("failed to create a PG Storage object: %w", err)
	}

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	tx, err := strg.Conn.Begin(ctx)
	if err != nil {
		return xerrors.Errorf("failed to BEGIN a transaction on sink %s: %w", Coordinator(), err)
	}
	rollbacks.Add(loggingRollbackTxFunc(ctx, tx))

	if csq := pgsink.CreateSchemaQueryOptional(ci.PgName()); len(csq) > 0 {
		if _, err := tx.Exec(ctx, csq); err != nil {
			logger.Log.Warn("Failed to execute CREATE SCHEMA IF NOT EXISTS query at table load initialization.", log.Error(err))
		}
	}

	if err := ensureTargetRandDistExists(ctx, ci, tx.Conn()); err != nil {
		return xerrors.Errorf("failed to ensure target table existence: %w", err)
	}

	if err := recreateTmpTable(ctx, ci, tx.Conn(), abstract.PgName(temporaryTable(ci.Schema, ci.Table))); err != nil {
		return xerrors.Errorf("failed to (re)create the temporary data transfer table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return xerrors.Errorf("failed to COMMIT a transaction on sink %s: %w", Coordinator(), err)
	}
	rollbacks.Cancel()
	return nil
}

func ensureTargetRandDistExists(ctx context.Context, schemaCI *abstract.ChangeItem, conn *pgx.Conn) error {
	var targetTableExists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, schemaCI.PgName()).Scan(&targetTableExists); err != nil {
		return xerrors.Errorf("failed to check existence of target table %s: %w", schemaCI.PgName(), err)
	}
	if targetTableExists {
		return nil
	}

	q, err := CreateRandDistTableQuery(schemaCI.PgName(), schemaCI.TableSchema.Columns())
	if err != nil {
		return xerrors.Errorf("failed to build a SQL query to create target table at destination: %w", err)
	}
	_, err = conn.Exec(ctx, q)
	if err != nil {
		return xerrors.Errorf("failed to execute a SQL query to create target table at destination: %w", err)
	}
	return nil
}

func CreateRandDistTableQuery(fullTableName string, schema []abstract.ColSchema) (string, error) {
	schemaWithoutPKs := make([]abstract.ColSchema, len(schema))
	for i := range schema {
		schemaWithoutPKs[i] = schema[i]
		schemaWithoutPKs[i].PrimaryKey = false
	}
	q, err := pgsink.CreateTableQuery(fullTableName, schemaWithoutPKs)
	if err != nil {
		return "", xerrors.Errorf("failed to build a CREATE TABLE SQL query: %w", err)
	}
	q = q + ` DISTRIBUTED RANDOMLY`

	return q, nil
}

func recreateTmpTable(ctx context.Context, schemaCI *abstract.ChangeItem, conn *pgx.Conn, tmpTableFQTN string) error {
	if _, err := conn.Exec(ctx, DropTableQuery(tmpTableFQTN)); err != nil {
		return xerrors.Errorf("failed to DROP a temporary table %s: %w", tmpTableFQTN, err)
	}
	q, err := CreateRandDistTableQuery(tmpTableFQTN, schemaCI.TableSchema.Columns())
	if err != nil {
		return xerrors.Errorf("failed to build a SQL query to create a temporary table at destination: %w", err)
	}
	_, err = conn.Exec(ctx, q)
	if err != nil {
		return xerrors.Errorf("failed to execute a SQL query to create a temporary table at destination: %w", err)
	}
	return nil
}

func (s *Sink) processDoneTableLoad(ctx context.Context, ci *abstract.ChangeItem) error {
	strg, err := s.sinks.PGStorage(ctx, Coordinator())
	if err != nil {
		return xerrors.Errorf("failed to create a PG Storage object: %w", err)
	}

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	tx, err := strg.Conn.Begin(ctx)
	if err != nil {
		return xerrors.Errorf("failed to BEGIN a transaction on sink %s: %w", Coordinator(), err)
	}
	rollbacks.Add(loggingRollbackTxFunc(ctx, tx))

	tmpTableFQTN := abstract.PgName(temporaryTable(ci.Schema, ci.Table))
	if err := copyTmpTableToTarget(ctx, ci, tx.Conn(), tmpTableFQTN); err != nil {
		return xerrors.Errorf("failed to migrate data from a temporary table %s to the target one %s: %w", tmpTableFQTN, ci.PgName(), err)
	}
	if _, err := tx.Exec(ctx, DropTableQuery(tmpTableFQTN)); err != nil {
		logger.Log.Warn(fmt.Sprintf("failed to DROP a temporary table %s", tmpTableFQTN), log.Error(err))
	}

	if err := tx.Commit(ctx); err != nil {
		return xerrors.Errorf("failed to COMMIT a transaction on sink %s: %w", Coordinator(), err)
	}
	rollbacks.Cancel()
	return nil
}

func copyTmpTableToTarget(ctx context.Context, schemaCI *abstract.ChangeItem, conn *pgx.Conn, tmpTableFQTN string) error {
	query := InsertFromSelectQuery(schemaCI.PgName(), tmpTableFQTN, InsertQueryColumns(schemaCI))
	if _, err := conn.Exec(ctx, query); err != nil {
		return xerrors.Errorf("failed to execute temporary table copy SQL: %w", err)
	}
	return nil
}

// InsertQueryColumns returns a set of columns (fields, not values) for an INSERT query. Auto-generated columns are removed from the result
func InsertQueryColumns(ci *abstract.ChangeItem) []string {
	result := make([]string, 0)
	for i := range ci.TableSchema.Columns() {
		columnSchema := ci.TableSchema.Columns()[i]
		if columnSchema.Expression != "" {
			// generated column, skip
			continue
		}
		result = append(result, fmt.Sprintf("\"%s\"", columnSchema.ColumnName))
	}
	return result
}

// InsertFromSelectQuery returns a `INSERT INTO ... SELECT FROM` SQL query
func InsertFromSelectQuery(tableDst string, tableSrc string, columnNames []string) string {
	return fmt.Sprintf(`INSERT INTO %[1]s(%[2]s) SELECT %[2]s FROM %[3]s`, tableDst, strings.Join(columnNames, ", "), tableSrc)
}

// DropTableQuery returns a `DROP TABLE IF EXISTS` SQL query. So the resulting query is "ensuring", not "imperative"
func DropTableQuery(tableFQTN string) string {
	return fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableFQTN)
}

func loggingRollbackTxFunc(ctx context.Context, tx pgx.Tx) func() {
	return func() {
		if err := tx.Rollback(ctx); err != nil {
			logger.Log.Warn("Failed while rolling back transaction in Greenplum", log.Error(err))
		}
	}
}
