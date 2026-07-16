package postgres

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres/pgerrors"
)

var (
	WALRemovedErr = &pgconn.PgError{
		Severity: "ERROR",
		Code:     string(pgerrors.ErrcUndefinedFile),
		Message:  "requested WAL segment pg_wal/0000000B00000007000000B0 has already been removed",
	}
)

func TestReceiveMessage(t *testing.T) {
	ctx := context.Background()

	conn := &mockConn{}
	slotMonitor := &SlotMonitor{conn: conn}
	mutexedConn := &mutexedPgConn{
		pgconn: &mockPgConn{},
		mutex:  &sync.Mutex{},
	}

	res, err := mutexedConn.ReceiveMessage(ctx, slotMonitor)
	require.Nil(t, res)
	require.ErrorIs(t, err, WALRemovedErr)
	require.True(t, abstract.IsFatal(err))
}

type mockPgConn struct {
	messageReceiver
}

func (m *mockPgConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return nil, context.DeadlineExceeded
}

type mockRows struct {
	pgx.Rows
	err error
}

func (r *mockRows) Next() bool {
	return false
}
func (r *mockRows) Close() {}

func (r *mockRows) Err() error {
	return r.err
}

type mockConn struct {
	QueryF func(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
	pgxtype.Querier
}

func (c *mockConn) Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error) {
	if c.QueryF != nil {
		return c.QueryF(ctx, sql, optionsAndArgs)
	}
	if !strings.Contains(sql, "SELECT pg_logical_slot_peek_changes") {
		return nil, xerrors.New("unexpected sql")
	}
	rows := &mockRows{err: WALRemovedErr}

	return rows, nil
}
