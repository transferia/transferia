//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

// BeginTx starts a transaction for the given pool with the given options and automatically composes sufficient rollback object.
func BeginTx(ctx context.Context, conn *pgx.Conn, options pgx.TxOptions, lgr log.Logger) (pgx.Tx, *util.Rollbacks, error) {
	tx, err := conn.BeginTx(ctx, options)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to BEGIN a transaction: %w", err)
	}

	rollbacks := util.Rollbacks{}
	rollbacks.Add(RollbackFuncForPgxTx(ctx, tx, lgr))

	return tx, &rollbacks, nil
}

// RollbackFuncForPgxTx returns a function which ROLLBACKs the given `pgx.Tx` and if an error happens, logs it with the given logger at WARNING level
func RollbackFuncForPgxTx(ctx context.Context, tx pgx.Tx, lgr log.Logger) func() {
	return func() {
		if err := tx.Rollback(ctx); err != nil {
			lgr.Warn("failed to ROLLBACK a transaction", log.Error(err))
		}
	}
}

// BeginTxWithSnapshot starts a transaction at the given connection with the given snapshot and automatically composes a sufficient rollback object.
//
// If the source database does not support SET TRANSACTION SNAPSHOT or the given snapshot identifier is an empty string, a transaction without snapshot is started automatically.
func BeginTxWithSnapshot(ctx context.Context, conn *pgx.Conn, options pgx.TxOptions, snapshot string, lgr log.Logger) (pgx.Tx, *util.Rollbacks, error) {
	tx, rollbacks, err := BeginTx(ctx, conn, options, lgr)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to start a transaction: %w", err)
	}

	if len(snapshot) == 0 {
		// snapshot shall not be set for this transaction
		return tx, rollbacks, nil
	}

	qry := fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshot)
	if _, err := tx.Exec(ctx, qry); err != nil {
		rollbacks.Do()
		return nil, nil, xerrors.Errorf("failed to execute %s: %w", qry, err)
	}

	return tx, rollbacks, nil
}

// CurrentTxStartTime returns the start time of the current transaction at the given connection. If there is no active transaction, current time of the database is returned.
func CurrentTxStartTime(ctx context.Context, conn *pgx.Conn) (time.Time, error) {
	var result time.Time
	if err := conn.QueryRow(ctx, "SELECT now()::TIMESTAMP WITH TIME ZONE").Scan(&result); err != nil {
		return time.UnixMicro(0), xerrors.Errorf("failed to execute SELECT to obtain current transaction start time: %w", err)
	}
	return result, nil
}
