//go:build !disable_postgres_provider

package dblog

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dblog"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type (
	txOp func(conn pgx.Tx) error
)

const (
	SignalTableName        = "__data_transfer_signal_table"
	tableSchemaColumnIndex = 0
	tableNameColumnIndex   = 1
	tableTransferIDIndex   = 2
	markColumnIndex        = 3
	markTypeColumnIndex    = 4
)

func SignalTableTableID(schemaName string) *abstract.TableID {
	return abstract.NewTableID(schemaName, SignalTableName)
}

type signalTable struct {
	conn       *pgxpool.Pool
	logger     log.Logger
	transferID string
	schemaName string
}

func buildSignalTableDDL(schemaName string) string {
	query := `CREATE TABLE IF NOT EXISTS "%s"."%s"
			  (
				  table_schema TEXT,
				  table_name TEXT,
				  transfer_id TEXT,
				  mark UUID,
				  mark_type CHAR,
				  low_bound TEXT,
				  PRIMARY KEY (table_schema, table_name, transfer_id, mark_type)
			  );`

	return fmt.Sprintf(query, schemaName, SignalTableName)
}

func NewPgSignalTable(
	ctx context.Context,
	conn *pgxpool.Pool,
	logger log.Logger,
	transferID string,
	schemaName string,
) (*signalTable, error) {
	pgSignalTable := &signalTable{
		conn:       conn,
		logger:     logger,
		transferID: transferID,
		schemaName: schemaName,
	}

	if err := pgSignalTable.init(ctx); err != nil {
		return nil, xerrors.Errorf("unable to initialize signal table: %w", err)
	}

	return pgSignalTable, nil
}

func (s *signalTable) init(ctx context.Context) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, buildSignalTableDDL(s.schemaName)); err != nil {
			return xerrors.Errorf("failed to ensure existence of the signal table service table: %w", err)
		}
		return nil
	})
}

func (s *signalTable) tx(ctx context.Context, operation txOp) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return xerrors.Errorf("unable to begin transaction: %w", err)
	}

	if err := operation(tx); err != nil {
		if err := tx.Rollback(ctx); err != nil {
			s.logger.Warn("Unable to rollback", log.Error(err))
		}
		return xerrors.Errorf("unable to execute operation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return xerrors.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

func (s *signalTable) CreateWatermark(
	ctx context.Context,
	tableID abstract.TableID,
	watermarkType dblog.WatermarkType,
	lowBoundArr []string,
) (uuid.UUID, error) {
	var newUUID uuid.UUID
	err := s.tx(ctx, func(conn pgx.Tx) error {
		tx, err := s.conn.Begin(ctx)
		if err != nil {
			return xerrors.Errorf("unable to begin transaction: %w", err)
		}

		rollback := util.Rollbacks{}
		defer rollback.Do()
		rollback.Add(func() {
			if err := tx.Rollback(ctx); err != nil {
				s.logger.Info("Unable to rollback", log.Error(err))
			}
		})

		newUUID = uuid.New()

		lowBoundStr, err := dblog.ConvertArrayToString(lowBoundArr)
		if err != nil {
			return xerrors.Errorf("unable to convert low bound array to string")
		}

		query := s.makeWatermarkQuery()
		s.logger.Info(
			fmt.Sprintf("CreateWatermark - query: %s", strings.ReplaceAll(query, "\n", "")),
			log.String("tableID.Namespace", tableID.Namespace),
			log.String("tableID.Name", tableID.Name),
			log.String("s.transferID", s.transferID),
			log.String("newUUID", newUUID.String()),
			log.String("watermarkType", string(watermarkType)),
			log.String("lowBoundStr", lowBoundStr),
		)

		_, err = tx.Exec(
			ctx,
			query,
			tableID.Namespace,
			tableID.Name,
			s.transferID,
			newUUID,
			string(watermarkType),
			lowBoundStr,
		)
		if err != nil {
			return xerrors.Errorf("failed to create watermark for %s: %w", tableID.Fqtn(), err)
		}

		if err := tx.Commit(ctx); err != nil {
			return xerrors.Errorf("unable to commit transaction: %w", err)
		}

		rollback.Cancel()
		return nil
	})

	return newUUID, err
}

func (s *signalTable) IsWatermark(item *abstract.ChangeItem, tableID abstract.TableID, markUUID uuid.UUID) (bool, dblog.WatermarkType) {
	isWatermark := item.Table == SignalTableName
	if !isWatermark {
		return false, dblog.BadWatermarkType
	}

	if item.Kind == abstract.DeleteKind ||
		item.ColumnValues[tableSchemaColumnIndex].(string) != tableID.Namespace ||
		item.ColumnValues[tableNameColumnIndex].(string) != tableID.Name ||
		item.ColumnValues[tableTransferIDIndex].(string) != s.transferID {
		return false, dblog.BadWatermarkType
	}

	parsedUUID, err := uuid.Parse(item.ColumnValues[markColumnIndex].(string))
	if err != nil {
		return true, dblog.BadWatermarkType
	}

	if parsedUUID != markUUID {
		return true, dblog.BadWatermarkType
	}

	if _, ok := item.ColumnValues[markTypeColumnIndex].(string); !ok {
		return false, dblog.BadWatermarkType
	}
	strVal := item.ColumnValues[markTypeColumnIndex].(string)
	if len(strVal) != 1 {
		return false, dblog.BadWatermarkType
	}

	return true, dblog.WatermarkType(strVal)
}

func (s *signalTable) makeWatermarkQuery() string {
	query := `INSERT INTO "%s"."%s" (table_schema, table_name, transfer_id, mark, mark_type, low_bound)
			  VALUES (($1), ($2), ($3), ($4), ($5), ($6))
			  ON CONFLICT (table_schema, table_name, transfer_id, mark_type)
			  DO UPDATE
			  SET mark = EXCLUDED.mark,
				  low_bound = EXCLUDED.low_bound;`

	return fmt.Sprintf(query, s.schemaName, SignalTableName)
}

func (s *signalTable) resolveLowBound(ctx context.Context, tableID abstract.TableID) []string {
	var lowBoundStr string

	err := s.tx(ctx, func(conn pgx.Tx) error {
		query := s.resolveLowBoundQuery()

		err := conn.QueryRow(ctx, query, tableID.Namespace, tableID.Name, s.transferID, dblog.SuccessWatermarkType).Scan(&lowBoundStr)
		if err != nil {
			return xerrors.Errorf("failed to retrieve low_bound for Namespace: %s, Table: %s, transferID: %s, err : %w", tableID.Namespace, tableID.Name, s.transferID, err)
		}

		return nil
	})
	if err != nil {
		return nil
	}

	lowBoundArray, err := dblog.ConvertStringToArray(lowBoundStr)
	if err != nil {
		return nil
	}

	return lowBoundArray
}

func (s *signalTable) resolveLowBoundQuery() string {
	query := `SELECT low_bound FROM "%s"."%s"
              WHERE table_schema = ($1)
              	AND table_name = ($2)
                AND transfer_id = ($3)
              	AND mark_type = ($4);`

	return fmt.Sprintf(query, s.schemaName, SignalTableName)
}

func DeleteWatermarks(ctx context.Context, conn *pgxpool.Pool, schemaName string, transferID string) error {
	signalTableExist, err := signalTableExist(ctx, conn, schemaName)
	if err != nil {
		return xerrors.Errorf("signal table check query failed err: %w", err)
	}
	if signalTableExist {
		query := deleteWatermarksQuery(schemaName)
		_, err := conn.Exec(ctx, query, transferID)
		if err != nil {
			return xerrors.Errorf("failed to delete watermarks err: %w", err)
		}
	}

	return err
}

func deleteWatermarksQuery(schemaName string) string {
	query := `DELETE FROM "%s"."%s" WHERE transfer_id = ($1);`
	return fmt.Sprintf(query, schemaName, SignalTableName)
}

func signalTableExist(ctx context.Context, conn *pgxpool.Pool, schemaName string) (bool, error) {
	query := `SELECT EXISTS (
		SELECT 1 
		FROM information_schema.tables 
		WHERE table_schema = $1 
		AND table_name = $2
	   );`

	var exist bool
	if err := conn.QueryRow(ctx, query, schemaName, SignalTableName).Scan(&exist); err != nil {
		return false, err
	}

	return true, nil
}
