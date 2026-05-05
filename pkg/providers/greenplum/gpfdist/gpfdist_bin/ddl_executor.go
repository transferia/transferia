package gpfdist_bin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type gpfdistDDLExecutor struct {
	conn          *pgxpool.Pool
	serviceSchema string
	extTableName  string
	tx            pgx.Tx
	cancelRun     context.CancelFunc
	runFinished   chan struct{}
}

func newGpfdistDDLExecutor(conn *pgxpool.Pool, table abstract.TableID, serviceSchema, tmpObjectsSuffix string) *gpfdistDDLExecutor {
	if serviceSchema == "" {
		serviceSchema = table.Namespace
	}
	extTableName := abstract.PgName(serviceSchema, tmpExtTableName(table.Name, tmpObjectsSuffix))
	return &gpfdistDDLExecutor{
		conn:          conn,
		serviceSchema: serviceSchema,
		extTableName:  extTableName,
		tx:            nil,
		cancelRun:     nil,
		runFinished:   make(chan struct{}),
	}
}

// Commit waits for run to finish and commits transaction.
// NOTE: Commit rollbacks transaction if error has occured.
func (d *gpfdistDDLExecutor) Commit(ctx context.Context) error {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	rollbacks.Add(func() { d.Rollback(ctx) })

	select {
	case <-d.runFinished:
	case <-ctx.Done():
		return xerrors.Errorf("context is done: %w", ctx.Err())
	}
	if d.tx == nil {
		return xerrors.New("transaction is nil")
	}
	err := d.tx.Commit(ctx)
	d.tx = nil // If commit failed, tx is already aborted by DB.
	if err != nil {
		return err
	}
	dropCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := d.dropExtTable(dropCtx); err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to drop ext table '%s'", d.extTableName), log.Error(err))
	}
	rollbacks.Cancel()
	return nil
}

// Rollback rollbacks transaction. Could be called many times, only first one matters.
func (d *gpfdistDDLExecutor) Rollback(ctx context.Context) {
	if d.cancelRun == nil {
		logger.Log.Error("DDL Executor cancel function is nil")
		return
	}
	d.cancelRun()
	<-d.runFinished
	if d.tx == nil {
		return
	}
	err := d.tx.Rollback(ctx)
	if err == nil {
		logger.Log.Info("Transaction rolled back")
		return
	}
	logger.Log.Error("Unable to rollback transaction", log.Error(err))
	if err := d.dropExtTable(ctx); err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to drop ext table '%s'", d.extTableName), log.Error(err))
	}
	d.tx = nil
}

func (d *gpfdistDDLExecutor) dropExtTable(ctx context.Context) error {
	_, err := d.conn.Exec(ctx, fmt.Sprintf("DROP EXTERNAL TABLE IF EXISTS %s", d.extTableName))
	return err
}

func (d *gpfdistDDLExecutor) runImpl(
	ctx context.Context, mode externalTableMode, table abstract.TableID,
	schema *abstract.TableSchema, locations []string,
) (int64, error) {
	defer close(d.runFinished)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	d.cancelRun = cancel

	if len(locations) == 0 {
		return 0, xerrors.New("locations is empty")
	}
	var sourceTableName, targetTableName string
	tableName := abstract.PgName(table.Namespace, table.Name)
	switch mode {
	case modeWritable:
		sourceTableName, targetTableName = tableName, d.extTableName
	case modeReadable:
		sourceTableName, targetTableName = d.extTableName, tableName
	}

	createExtTableQuery, err := buildCreateExtTableQuery(d.extTableName, mode, locations, schema)
	if err != nil {
		return 0, xerrors.Errorf("unable to generate external table creation query: %w", err)
	}

	if err := d.dropExtTable(ctx); err != nil {
		return 0, xerrors.Errorf("Unable to drop external table '%s' on startup: %w", d.extTableName, err)
	}

	tx, err := d.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		return 0, xerrors.Errorf("unable to begin transaction: %w", err)
	}
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	rollbacks.Add(func() {
		if err := tx.Rollback(ctx); err != nil {
			logger.Log.Error("Unable to rollback tx", log.Error(err))
		}
	})

	logger.Log.Info("Creating external table", log.String("sql", createExtTableQuery))
	if _, err := tx.Exec(ctx, createExtTableQuery); err != nil {
		msg := "Unable to create external table"
		logger.Log.Error(msg, log.Error(err), log.String("sql", createExtTableQuery))
		return 0, xerrors.Errorf("%s: %w", msg, err)
	}
	selectAndInsertQuery := buildSelectAndInsertQuery(sourceTableName, targetTableName, schema)
	tag, err := tx.Exec(ctx, selectAndInsertQuery)
	if err != nil {
		msg := fmt.Sprintf("Unable to select and insert with external %s table", string(mode))
		logger.Log.Error(msg, log.Error(err), log.String("sql", selectAndInsertQuery))
		lower := strings.ToLower(err.Error())
		if util.ContainsAnySubstrings(
			lower,
			"external table has more urls than available primary segments",
			"more urls than segments",
			"more urls than available primary segments",
		) {
			return 0, coded.Errorf(error_codes.GreenplumExternalUrlsExceedSegments, "%s: %w", msg, err)
		}
		return 0, xerrors.Errorf("%s: %w", msg, err)
	}
	d.tx = tx
	rollbacks.Cancel()

	rowsCount := tag.RowsAffected()
	logger.Log.Infof("Inserted %d rows from %s to %s", rowsCount, sourceTableName, targetTableName)
	return rowsCount, nil
}

func buildCreateExtTableQuery(
	fullTableName string, mode externalTableMode, locations []string, schema *abstract.TableSchema,
) (string, error) {
	columns := schema.Columns()
	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("CREATE %s EXTERNAL TABLE %s (\n", string(mode), fullTableName))
	for i, col := range columns {
		if i > 0 {
			query.WriteString(",\n")
		}
		colType := ""
		if col.OriginalType != "" {
			colType = strings.TrimPrefix(col.OriginalType, "pg:")
			colType = strings.ReplaceAll(colType, "USER-DEFINED", "TEXT")
		} else {
			var err error
			colType, err = provider_postgres.DataToOriginal(col.DataType)
			if err != nil {
				return "", xerrors.Errorf("unable to convert column %s to GP type: %w", col.ColumnName, err)
			}
		}
		query.WriteString(fmt.Sprintf(`"%s" %s`, col.ColumnName, colType))
	}
	query.WriteString("\n)\n")
	query.WriteString(fmt.Sprintf("LOCATION ('%s')\n", strings.Join(locations, "','")))
	query.WriteString("FORMAT 'CSV' (DELIMITER E'\\t')\n")
	query.WriteString("ENCODING 'UTF8'")
	return query.String(), nil
}

func buildSelectAndInsertQuery(sourceTable, targetTable string, schema *abstract.TableSchema) string {
	columns := strings.Builder{}
	for _, col := range schema.Columns() {
		if columns.Len() > 0 {
			columns.WriteRune(',')
		}
		columns.WriteString(fmt.Sprintf(`"%s"`, col.ColumnName))
	}
	columnsString := columns.String()
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", targetTable, columnsString, columnsString, sourceTable)
}

func tmpExtTableName(name string, tmpObjectsSuffix string) string {
	return "_dt_" + name + "_" + tmpObjectsSuffix + "_ext"
}
