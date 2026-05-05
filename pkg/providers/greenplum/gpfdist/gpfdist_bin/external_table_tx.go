package gpfdist_bin

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type TxCloser interface {
	// Commit waits for run to finish and commits transaction.
	// NOTE: Commit rollbacks transaction if error has occured.
	Commit(context.Context) error

	// Rollback rollbacks transaction. Could be called many times, only first one matters.
	// Does nothing if called after commit.
	Rollback(context.Context)
}

type externalTableMode string

const (
	modeWritable = externalTableMode("WRITABLE")
	modeReadable = externalTableMode("READABLE")
)

type RunResult struct {
	Rows int64
	Err  error
}

// RunExternalTableTransaction executes the external table operation but keeps transaction open.
// One of Commit or Rollback must be called to clean DDLExecutor's run properly.
func RunExternalTableTransaction(
	ctx context.Context, mode externalTableMode, tableID abstract.TableID,
	tableSchema *abstract.TableSchema, locations []string,
	conn *pgxpool.Pool, serviceSchema, tmpObjectsSuffix string,
) (TxCloser, <-chan RunResult) {
	ddlExecutor := newGpfdistDDLExecutor(conn, tableID, serviceSchema, tmpObjectsSuffix)
	resCh := make(chan RunResult, 1)
	go func() {
		defer close(resCh)
		rows, err := ddlExecutor.runImpl(ctx, mode, tableID, tableSchema, locations)
		resCh <- RunResult{Rows: rows, Err: err}
	}()
	return ddlExecutor, resCh
}

// RunAndCommitExternalTableTransaction is RunExternalTableTransaction
// but waits execution and calls Commit (or Rollback if error occurs).
func RunAndCommitExternalTableTransaction(
	ctx context.Context, mode externalTableMode, tableID abstract.TableID,
	tableSchema *abstract.TableSchema, locations []string,
	conn *pgxpool.Pool, serviceSchema, tmpObjectsSuffix string,
) (int64, error) {
	closer, resCh := RunExternalTableTransaction(ctx, mode, tableID, tableSchema, locations, conn, serviceSchema, tmpObjectsSuffix)
	res := <-resCh
	if res.Err != nil {
		closer.Rollback(ctx)
		return 0, xerrors.Errorf("unable to run tx: %w", res.Err)
	}
	if err := closer.Commit(ctx); err != nil {
		return 0, xerrors.Errorf("unable to commit tx: %w", err)
	}
	return res.Rows, nil
}
