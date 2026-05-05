package pgx_mocks

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

// FakeTx is a fake in-memory implementation of pgx.Tx for testing.
// It tracks transaction state (committed/rolled back) and can simulate errors.
//
// Example usage:
//
//	fakeTx := &pgx_mocks.FakeTx{}
//	err := fakeTx.Commit(ctx)
//	if fakeTx.IsCommitted() {
//	    // transaction was committed
//	}
//
// To simulate errors:
//
//	fakeTx := &pgx_mocks.FakeTx{}
//	fakeTx.SetCommitError(errors.New("commit failed"))
//	err := fakeTx.Commit(ctx) // returns error
type FakeTx struct {
	committed   bool
	rolledBack  bool
	commitErr   error
	rollbackErr error
}

func (f *FakeTx) SetCommitError(err error) {
	f.commitErr = err
}

func (f *FakeTx) SetRollbackError(err error) {
	f.rollbackErr = err
}

func (f *FakeTx) IsCommitted() bool {
	return f.committed
}

func (f *FakeTx) IsRolledBack() bool {
	return f.rolledBack
}

func (f *FakeTx) Commit(ctx context.Context) error {
	if f.committed || f.rolledBack {
		return pgx.ErrTxClosed
	}
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = true
	return nil
}

func (f *FakeTx) Rollback(ctx context.Context) error {
	if f.rolledBack || f.committed {
		return pgx.ErrTxClosed
	}
	if f.rollbackErr != nil {
		return f.rollbackErr
	}
	f.rolledBack = true
	return nil
}

// Unused methods (no-op implementations for interface compliance)
func (f *FakeTx) Begin(ctx context.Context) (pgx.Tx, error)                  { return nil, nil }
func (f *FakeTx) BeginFunc(ctx context.Context, fn func(pgx.Tx) error) error { return nil }
func (f *FakeTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (f *FakeTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (f *FakeTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (f *FakeTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (f *FakeTx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (f *FakeTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return nil, nil
}
func (f *FakeTx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return nil
}
func (f *FakeTx) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, fn func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (f *FakeTx) Conn() *pgx.Conn { return nil }
