package greenplum

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/greenplum/gpfdist"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
)

func closedChan() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// FakeTxCloser is a fake implementation of gpfdistbin.TxCloser for testing.
type FakeTxCloser struct {
	committed      bool
	rolledBack     bool
	commitErr      error
	commitCalled   int
	rollbackCalled int
}

func (f *FakeTxCloser) SetCommitError(err error) {
	f.commitErr = err
}

func (f *FakeTxCloser) IsCommitted() bool {
	return f.committed
}

func (f *FakeTxCloser) IsRolledBack() bool {
	return f.rolledBack
}

func (f *FakeTxCloser) Commit(ctx context.Context) error {
	f.commitCalled++
	if f.commitErr != nil {
		f.Rollback(ctx)
		return f.commitErr
	}
	f.committed = true
	return nil
}

func (f *FakeTxCloser) Rollback(ctx context.Context) {
	f.rollbackCalled++
	if !f.committed {
		f.rolledBack = true
	}
}

// TestCommitTransaction_Success tests successful transaction commit
func TestCommitTransaction_Success(t *testing.T) {
	ctx := context.Background()
	fakeTxCloser := &FakeTxCloser{}
	sink := &GpfdistTableSink{
		extTableTxCloser:       fakeTxCloser,
		extTableWriterRes:      gpfdistbin.RunResult{Rows: 150, Err: nil},
		extTableWriterFinished: closedChan(),
		extTableWriterCancel:   func() {},
	}

	err := sink.commitTransaction(ctx)
	require.NoError(t, err)
	require.True(t, fakeTxCloser.IsCommitted())
	require.False(t, fakeTxCloser.IsRolledBack())
}

// TestCommitTransaction_ExternalWriterError tests handling of external writer error
func TestCommitTransaction_ExternalWriterError(t *testing.T) {
	ctx := context.Background()
	fakeTxCloser := &FakeTxCloser{}
	writerErr := xerrors.New("external writer failed to load data")
	sink := &GpfdistTableSink{
		extTableTxCloser:       fakeTxCloser,
		extTableWriterRes:      gpfdistbin.RunResult{Rows: 0, Err: writerErr},
		extTableWriterFinished: closedChan(),
		extTableWriterCancel:   func() {},
	}

	err := sink.commitTransaction(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "external writer failed")
	require.False(t, fakeTxCloser.IsCommitted())
	require.True(t, fakeTxCloser.IsRolledBack())
}

// TestCommitTransaction_RollbackOnError tests that rollback is called when writer fails
func TestCommitTransaction_RollbackOnError(t *testing.T) {
	ctx := context.Background()
	fakeTxCloser := &FakeTxCloser{}
	writerErr := xerrors.New("writer failed")
	sink := &GpfdistTableSink{
		extTableTxCloser:       fakeTxCloser,
		extTableWriterRes:      gpfdistbin.RunResult{Rows: 0, Err: writerErr},
		extTableWriterFinished: closedChan(),
		extTableWriterCancel:   func() {},
	}

	err := sink.commitTransaction(ctx)
	require.Error(t, err)
	require.True(t, fakeTxCloser.IsRolledBack())
	require.False(t, fakeTxCloser.IsCommitted())
}

// TestCommitTransaction_CommitFails tests rollback when commit fails
func TestCommitTransaction_CommitFails(t *testing.T) {
	ctx := context.Background()
	fakeTxCloser := &FakeTxCloser{}
	commitErr := xerrors.New("commit error")
	fakeTxCloser.SetCommitError(commitErr)
	sink := &GpfdistTableSink{
		extTableTxCloser:       fakeTxCloser,
		extTableWriterRes:      gpfdistbin.RunResult{Rows: 200, Err: nil},
		extTableWriterFinished: closedChan(),
		extTableWriterCancel:   func() {},
	}

	err := sink.commitTransaction(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to commit transaction")
	require.False(t, fakeTxCloser.IsCommitted())
	require.True(t, fakeTxCloser.IsRolledBack())
}

// TestClose_RollbackOnIncompleteLoad tests rollback when Close is called without commit
func TestClose_RollbackOnIncompleteLoad(t *testing.T) {
	ctx := context.Background()
	fakeTxCloser := &FakeTxCloser{}
	sink := &GpfdistTableSink{
		pipesWriters:         []*gpfdist.PipeWriter{},
		gpfdists:             []*gpfdistbin.Gpfdist{},
		extTableTxCloser:     fakeTxCloser,
		extTableWriterCancel: func() {},
	}

	err := sink.Close(ctx, false)
	require.NoError(t, err)
	require.False(t, fakeTxCloser.IsCommitted())
	require.True(t, fakeTxCloser.IsRolledBack())
}

// TestClose_AlwaysCallsRollback tests that rollback is always called in Close
func TestClose_AlwaysCallsRollback(t *testing.T) {
	ctx := context.Background()
	fakeTxCloser := &FakeTxCloser{}
	sink := &GpfdistTableSink{
		pipesWriters:         []*gpfdist.PipeWriter{},
		gpfdists:             []*gpfdistbin.Gpfdist{},
		extTableTxCloser:     fakeTxCloser,
		extTableWriterCancel: func() {},
	}

	err := sink.Close(ctx, false)
	require.NoError(t, err)
	require.True(t, fakeTxCloser.rollbackCalled > 0)
}

// TestClose_HandlesExternalWriterError tests that Close always rolls back regardless of writer state
func TestClose_HandlesExternalWriterError(t *testing.T) {
	ctx := context.Background()
	fakeTxCloser := &FakeTxCloser{}
	sink := &GpfdistTableSink{
		pipesWriters:         []*gpfdist.PipeWriter{},
		gpfdists:             []*gpfdistbin.Gpfdist{},
		extTableTxCloser:     fakeTxCloser,
		extTableWriterCancel: func() {},
	}

	err := sink.Close(ctx, false)
	require.NoError(t, err)
	require.True(t, fakeTxCloser.IsRolledBack())
}

// TestClose_CommitOnDoneTableLoad tests that Close with needCommit=true commits the transaction
func TestClose_CommitOnDoneTableLoad(t *testing.T) {
	ctx := context.Background()
	fakeTxCloser := &FakeTxCloser{}
	sink := &GpfdistTableSink{
		pipesWriters:           []*gpfdist.PipeWriter{},
		gpfdists:               []*gpfdistbin.Gpfdist{},
		extTableTxCloser:       fakeTxCloser,
		extTableWriterRes:      gpfdistbin.RunResult{Rows: 100, Err: nil},
		extTableWriterFinished: closedChan(),
		extTableWriterCancel:   func() {},
	}

	err := sink.Close(ctx, true)
	require.NoError(t, err)
	require.True(t, fakeTxCloser.IsCommitted())
}

// TestGpfdistSink_commitAndRemoveTableSink_NotExists tests error when table sink not found
func TestGpfdistSink_commitAndRemoveTableSink_NotExists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sink := &GpfdistSink{tableSinks: make(map[abstract.TableID]*GpfdistTableSink), tableSinksMu: sync.RWMutex{}}
	tableID := abstract.TableID{Namespace: "public", Name: "test"}

	err := sink.commitAndRemoveTableSink(ctx, tableID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not exists")
}

// TestGpfdistSink_commitAndRemoveTableSink_RemovesOnSuccess tests that the table sink is removed after commit
func TestGpfdistSink_commitAndRemoveTableSink_RemovesOnSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fakeTxCloser := &FakeTxCloser{}
	sink := &GpfdistSink{tableSinks: make(map[abstract.TableID]*GpfdistTableSink), tableSinksMu: sync.RWMutex{}}
	tableID := abstract.TableID{Namespace: "public", Name: "test"}

	sink.tableSinks[tableID] = &GpfdistTableSink{
		pipesWriters:           []*gpfdist.PipeWriter{},
		gpfdists:               []*gpfdistbin.Gpfdist{},
		extTableTxCloser:       fakeTxCloser,
		extTableWriterRes:      gpfdistbin.RunResult{Rows: 50, Err: nil},
		extTableWriterFinished: closedChan(),
		extTableWriterCancel:   func() {},
	}

	require.Len(t, sink.tableSinks, 1)
	require.NoError(t, sink.commitAndRemoveTableSink(ctx, tableID))
	require.Len(t, sink.tableSinks, 0)
	require.True(t, fakeTxCloser.IsCommitted())
}

func TestGpfdistSink_ConcurrentAccess(t *testing.T) {
	sink := &GpfdistSink{tableSinks: make(map[abstract.TableID]*GpfdistTableSink), tableSinksMu: sync.RWMutex{}}
	tableID := abstract.TableID{Namespace: "public", Name: "test"}
	sink.tableSinks[tableID] = &GpfdistTableSink{
		pipesWriters:         []*gpfdist.PipeWriter{},
		gpfdists:             []*gpfdistbin.Gpfdist{},
		extTableTxCloser:     &FakeTxCloser{},
		extTableWriterCancel: func() {},
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink.tableSinksMu.RLock()
			_ = sink.tableSinks[tableID]
			sink.tableSinksMu.RUnlock()
		}()
	}
	wg.Wait()
}
