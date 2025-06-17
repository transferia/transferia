//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type messageReceiver interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	Close(ctx context.Context) error
	Exec(ctx context.Context, sql string) *pgconn.MultiResultReader
}

type mutexedPgConn struct {
	pgconn messageReceiver
	mutex  *sync.Mutex
}

func newMutexedPgConn(conn *pgconn.PgConn) *mutexedPgConn {
	return &mutexedPgConn{
		pgconn: conn,
		mutex:  &sync.Mutex{},
	}
}

func (m *mutexedPgConn) Close(ctx context.Context) error {
	defer m.lockSelf("Close")()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return m.pgconn.Close(ctx)
}

func (m *mutexedPgConn) ReceiveMessage(ctx context.Context, slotMonitor *SlotMonitor) (pgproto3.BackendMessage, error) {
	var result pgproto3.BackendMessage

	const initialReceiveTimeout time.Duration = 1 * time.Minute

	receiveBackoff := backoff.NewExponentialBackOff()
	receiveBackoff.InitialInterval = initialReceiveTimeout
	receiveBackoff.MaxElapsedTime = 2 * time.Hour
	ctxBackoff := backoff.WithContext(receiveBackoff, ctx)

	currentCtxInterval := initialReceiveTimeout
	if backoffedErr := backoff.Retry(func() error {
		defer m.lockSelf("ReceiveMessage")()

		receiveCtx, cancel := context.WithTimeout(ctx, currentCtxInterval)
		defer cancel()
		msg, err := m.pgconn.ReceiveMessage(receiveCtx)
		if err != nil {
			if validateErr := slotMonitor.validateSlot(ctx); validateErr != nil {
				err = validateErr
			}
			if xerrors.Is(err, context.DeadlineExceeded) {
				currentCtxInterval = time.Duration(float64(currentCtxInterval) * receiveBackoff.Multiplier)
				return xerrors.Errorf("ReceiveMessage deadline exceeded: %w", err)
			}
			return backoff.Permanent(err)
		}
		result = msg
		return nil
	}, ctxBackoff); backoffedErr != nil {
		return nil, xerrors.Errorf("failed to ReceiveMessage: %w", backoffedErr)
	}
	return result, nil
}

func (m *mutexedPgConn) SendStandbyStatusUpdate(ctx context.Context, statusUpdate pglogrepl.StandbyStatusUpdate) error {
	defer m.lockSelf("SendStandbyStatusUpdate")()

	conn, ok := m.pgconn.(*pgconn.PgConn)
	if !ok {
		return xerrors.Errorf("unexpected type: %T", m.pgconn)
	}
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, statusUpdate)
}

func (m *mutexedPgConn) StartReplication(ctx context.Context, slotName string, startLSN pglogrepl.LSN, startReplicationOptions pglogrepl.StartReplicationOptions) error {
	defer m.lockSelf("StartReplication")()

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	return pglogrepl.StartReplication(ctx, m.pgconn.(*pgconn.PgConn), slotName, startLSN, startReplicationOptions)
}

func (m *mutexedPgConn) Exec(ctx context.Context, sql string) *pgconn.MultiResultReader {
	defer m.lockSelf("Exec")()

	return m.pgconn.Exec(ctx, sql)
}

func (m *mutexedPgConn) lockSelf(method string) (unlockFn func()) {
	mutexLockCalledAt := time.Now()
	m.mutex.Lock()
	mutexLockedAt := time.Now()
	return func() {
		m.mutex.Unlock()
		logCallTimings(method, mutexLockCalledAt, mutexLockedAt)
	}
}

func logCallTimings(callName string, mutexLockCalledAt, mutexLockedAt time.Time) {
	callFinishedAt := time.Now()
	if callFinishedAt.Sub(mutexLockCalledAt) <= 10*time.Second {
		return // Do not log fast calls
	}
	logger.Log.Info("Call timings", log.String("call_name", callName), log.Duration("mutex_wait_time", mutexLockedAt.Sub(mutexLockCalledAt)), log.Duration("call_time", callFinishedAt.Sub(mutexLockedAt)))
}
