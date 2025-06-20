//go:build !disable_greenplum_provider

package gpfdistbin

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type externalTableMode string

const (
	modeWritable = externalTableMode("WRITABLE")
	modeReadable = externalTableMode("READABLE")
)

type GpfdistDDLExecutor struct {
	conn *pgxpool.Pool
}

func temporaryTable(schema string, name string) (ttSchema string, ttName string) {
	return schema, "_dt_" + name
}

func (d *GpfdistDDLExecutor) createExternalTableAndInsertRows(
	ctx context.Context, mode externalTableMode, table abstract.TableID,
	schema *abstract.TableSchema, serviceSchema string, locations []string,
) (int64, error) {
	if serviceSchema == "" {
		serviceSchema = table.Namespace
	}
	var sourceTableName, targetTableName string
	tableName := abstract.PgName(table.Namespace, table.Name)
	externalTableName := abstract.PgName(temporaryTable(serviceSchema, table.Name+"__ext"))
	switch mode {
	case modeWritable:
		sourceTableName, targetTableName = tableName, externalTableName
	case modeReadable:
		sourceTableName, targetTableName = externalTableName, tableName
	}

	createExtTableQuery, err := d.buildCreateExtTableQuery(externalTableName, mode, locations, schema)
	if err != nil {
		return 0, xerrors.Errorf("unable to generate external table creation query: %w", err)
	}
	selectAndInsertQuery := d.buildSelectAndInsertQuery(sourceTableName, targetTableName, schema)

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

	if _, err := tx.Exec(ctx, createExtTableQuery); err != nil {
		msg := "Unable to create external table"
		logger.Log.Error(msg, log.Error(err), log.String("sql", createExtTableQuery))
		return 0, xerrors.Errorf("%s: %w", msg, err)
	}
	defer func() {
		dropTableQuery := fmt.Sprintf("DROP EXTERNAL TABLE %s", externalTableName)
		if _, err := d.conn.Exec(ctx, dropTableQuery); err != nil {
			logger.Log.Error("Unable to drop external table", log.Error(err), log.String("sql", dropTableQuery))
		} else {
			logger.Log.Debugf("External table %s dropped", externalTableName)
		}
	}()

	tag, err := tx.Exec(ctx, selectAndInsertQuery)
	if err != nil {
		msg := fmt.Sprintf("Unable to select and insert with external %s table", string(mode))
		logger.Log.Error(msg, log.Error(err), log.String("sql", selectAndInsertQuery))
		return 0, xerrors.Errorf("%s: %w", msg, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, xerrors.Errorf("Unable to commit external %s table transaction: %w", string(mode), err)
	}
	rollbacks.Cancel()

	rowsCount := tag.RowsAffected()
	logger.Log.Debugf("Inserted %d rows from %s to %s", rowsCount, sourceTableName, targetTableName)
	return rowsCount, nil
}

func (d *GpfdistDDLExecutor) buildCreateExtTableQuery(
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
			colType, err = postgres.DataToOriginal(col.DataType)
			if err != nil {
				return "", xerrors.Errorf("unable to convert column %s to GP type: %w", col.ColumnName, err)
			}
		}
		query.WriteString(fmt.Sprintf(`"%v" %v`, col.ColumnName, colType))
	}
	query.WriteString("\n)\n")
	query.WriteString(fmt.Sprintf("LOCATION ('%s')\n", strings.Join(locations, "','")))
	query.WriteString("FORMAT 'CSV' (DELIMITER E'\\t')\n")
	query.WriteString("ENCODING 'UTF8'")
	return query.String(), nil
}

func (d *GpfdistDDLExecutor) buildSelectAndInsertQuery(sourceTable, targetTable string, schema *abstract.TableSchema) string {
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

func NewGpfdistDDLExecutor(conn *pgxpool.Pool) *GpfdistDDLExecutor {
	return &GpfdistDDLExecutor{conn: conn}
}
