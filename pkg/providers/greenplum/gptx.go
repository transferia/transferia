//go:build !disable_greenplum_provider

package greenplum

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

// gpTx is a transaction with a connection
type gpTx struct {
	tx      pgx.Tx
	txMutex sync.Mutex
	conn    *pgxpool.Conn
	closed  bool
}

func newGpTx(ctx context.Context, storage *postgres.Storage) (*gpTx, error) {
	conn, err := storage.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection: %w", err)
	}
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	rollbacks.Add(func() {
		conn.Release()
	})

	if _, err := conn.Exec(ctx, postgres.MakeSetSQL("statement_timeout", "0")); err != nil {
		return nil, xerrors.Errorf("failed to SET statement_timeout: %w", err)
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.Deferrable,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to start a cluster-wide transaction: %w", err)
	}

	rollbacks.Cancel()
	return &gpTx{
		tx:      tx,
		txMutex: sync.Mutex{},
		conn:    conn,
		closed:  false,
	}, nil
}

func (s *gpTx) withConnection(f func(conn *pgx.Conn) error) error {
	s.txMutex.Lock()
	defer s.txMutex.Unlock()
	return f(s.tx.Conn())
}

// CloseRollback ROLLBACKs the transaction
func (s *gpTx) CloseRollback(ctx context.Context) error {
	if s.closed {
		return nil
	}

	err := s.tx.Rollback(ctx)
	s.conn.Release()
	s.closed = true

	if err != nil {
		return xerrors.Errorf("failed to rollback transaction: %w", err)
	}
	return nil
}

// CloseCommit first tries to COMMIT transaction. If an error is encountered, the transaction is ROLLBACKed
func (s *gpTx) CloseCommit(ctx context.Context) error {
	if s.closed {
		return nil
	}

	result := s.tx.Commit(ctx)
	if result == nil {
		s.conn.Release()
		s.closed = true
		return nil
	}
	result = xerrors.Errorf("failed to commit transaction: %w", result)

	if err := s.CloseRollback(ctx); err != nil {
		logger.Log.Warn("Failed to rollback transaction in Greenplum", log.Error(err))
	}

	return result
}
