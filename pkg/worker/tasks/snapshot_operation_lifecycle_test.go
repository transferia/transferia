package tasks

import (
	"context"
	stderrors "errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	mockstorage "github.com/transferia/transferia/tests/helpers/mock_storage"
	"go.ytsaurus.tech/library/go/core/log"
)

const snapshotOperationTestProviderType = abstract.ProviderType("snapshot-operation-lifecycle-test")

type snapshotOperationTestDestination struct {
	model.MockDestination
	closeOperation func()
}

var (
	_ model.Destination                 = (*snapshotOperationTestDestination)(nil)
	_ providers.Sinker                  = (*snapshotOperationTestDestination)(nil)
	_ providers.SnapshotOperationCloser = (*snapshotOperationTestDestination)(nil)
)

func init() {
	providers.Register(snapshotOperationTestProviderType, func(_ log.Logger, _ core_metrics.Registry, _ coordinator.Coordinator, transfer *model.Transfer, _ *model.TransferOperation) providers.Provider {
		return transfer.Dst.(*snapshotOperationTestDestination)
	})
}

func (d *snapshotOperationTestDestination) Type() abstract.ProviderType {
	return snapshotOperationTestProviderType
}

func (d *snapshotOperationTestDestination) GetProviderType() abstract.ProviderType {
	return snapshotOperationTestProviderType
}

func (d *snapshotOperationTestDestination) Sink(_ middlewares.Config) (abstract.Sinker, error) {
	return d.SinkerFactory(), nil
}

func (d *snapshotOperationTestDestination) CloseSnapshotOperation() {
	d.closeOperation()
}

type snapshotOperationTestSink struct {
	closes *atomic.Int32
}

var _ abstract.Sinker = (*snapshotOperationTestSink)(nil)

func (s *snapshotOperationTestSink) Push(_ []abstract.ChangeItem) error {
	return nil
}

func (s *snapshotOperationTestSink) Close() error {
	s.closes.Add(1)
	return nil
}

func newSnapshotOperationTestLoader(storage abstract.Storage, closeOperation func(), sinkCloses *atomic.Int32) *SnapshotLoader {
	destination := new(snapshotOperationTestDestination)
	destination.Cleanup = model.DisabledCleanup
	destination.SinkerFactory = func() abstract.Sinker {
		return &snapshotOperationTestSink{closes: sinkCloses}
	}
	destination.closeOperation = closeOperation
	source := new(model.MockSource)
	source.StorageFactory = func() abstract.Storage { return storage }
	runtime := new(abstract.LocalRuntime)
	runtime.ShardingUpload.JobCount = 1
	runtime.ShardingUpload.ProcessCount = 1
	transfer := new(model.Transfer)
	transfer.Src = source
	transfer.Dst = destination
	transfer.Runtime = runtime
	operation := new(model.TransferOperation)
	operation.OperationID = "operation-lifecycle"
	return NewSnapshotLoader(coordinator.NewStatefulFakeClient(), operation, transfer, solomon.NewRegistry(nil))
}

func snapshotOperationTestTables() []abstract.TableDescription {
	table := new(abstract.TableDescription)
	table.Name = "rows"
	return []abstract.TableDescription{*table}
}

func TestSnapshotOperationClosesAfterUpload(test *testing.T) {
	for _, failUpload := range []bool{false, true} {
		name := "success"
		if failUpload {
			name = "upload_error"
		}
		test.Run(name, func(test *testing.T) {
			var ownerCloses atomic.Int32
			var sinkCloses atomic.Int32
			var loadReturned atomic.Bool
			var ownerClosedAfterLoad atomic.Bool
			var loadSawServiceSinkClose atomic.Bool
			uploadFailure := stderrors.New("upload failed")
			storage := mockstorage.NewMockStorage()
			storage.LoadTableF = func(_ context.Context, _ abstract.TableDescription, _ abstract.Pusher) error {
				defer loadReturned.Store(true)
				loadSawServiceSinkClose.Store(sinkCloses.Load() > 0 && ownerCloses.Load() == 0)
				if failUpload {
					return abstract.NewFatalError(uploadFailure)
				}
				return nil
			}
			loader := newSnapshotOperationTestLoader(storage, func() {
				ownerClosedAfterLoad.Store(loadReturned.Load())
				ownerCloses.Add(1)
			}, &sinkCloses)

			err := loader.UploadTables(context.Background(), snapshotOperationTestTables(), false)
			if failUpload {
				require.ErrorIs(test, err, uploadFailure)
			} else {
				require.NoError(test, err)
			}
			require.True(test, loadSawServiceSinkClose.Load(), "closing the init service sink must leave operation resources alive")
			require.True(test, ownerClosedAfterLoad.Load())
			require.EqualValues(test, 1, ownerCloses.Load())
		})
	}
}

func TestSnapshotOperationClosesAfterCancelledUploadReturns(test *testing.T) {
	var ownerCloses atomic.Int32
	var sinkCloses atomic.Int32
	var loadReturned atomic.Bool
	var ownerClosedAfterLoad atomic.Bool
	loadStarted := make(chan struct{})
	storage := mockstorage.NewMockStorage()
	storage.LoadTableF = func(ctx context.Context, _ abstract.TableDescription, _ abstract.Pusher) error {
		defer loadReturned.Store(true)
		close(loadStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	loader := newSnapshotOperationTestLoader(storage, func() {
		ownerClosedAfterLoad.Store(loadReturned.Load())
		ownerCloses.Add(1)
	}, &sinkCloses)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- loader.UploadTables(ctx, snapshotOperationTestTables(), false)
	}()
	select {
	case <-loadStarted:
	case err := <-done:
		test.Fatalf("upload stopped before loading a table: %v", err)
	case <-time.After(5 * time.Second):
		test.Fatal("upload did not start")
	}
	require.Zero(test, ownerCloses.Load())
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(test, err, context.Canceled)
	case <-time.After(5 * time.Second):
		test.Fatal("cancelled upload did not finish")
	}
	require.True(test, ownerClosedAfterLoad.Load())
	require.EqualValues(test, 1, ownerCloses.Load())
}

type snapshotOperationTestCoordinator struct {
	coordinator.Coordinator
	createWorkersError error
}

var _ coordinator.Coordinator = (*snapshotOperationTestCoordinator)(nil)

func (c *snapshotOperationTestCoordinator) CreateOperationWorkers(_ string, _ int) error {
	return c.createWorkersError
}

func TestSnapshotOperationClosesOnMainWorkerEarlyReturn(test *testing.T) {
	for _, failBeforeInit := range []bool{false, true} {
		name := "create_workers_error"
		if failBeforeInit {
			name = "no_tables"
		}
		test.Run(name, func(test *testing.T) {
			var ownerCloses atomic.Int32
			var sinkCloses atomic.Int32
			loader := newSnapshotOperationTestLoader(mockstorage.NewMockStorage(), func() {
				ownerCloses.Add(1)
			}, &sinkCloses)
			loader.transfer.Runtime.(*abstract.LocalRuntime).ShardingUpload.JobCount = 2
			workersFailure := stderrors.New("cannot create workers")
			loader.cp = &snapshotOperationTestCoordinator{
				Coordinator:        loader.cp,
				createWorkersError: workersFailure,
			}
			tables := snapshotOperationTestTables()
			if failBeforeInit {
				tables = nil
			}

			err := loader.UploadTables(context.Background(), tables, false)
			if failBeforeInit {
				require.ErrorContains(test, err, "no tables in snapshot")
				require.Zero(test, sinkCloses.Load())
			} else {
				require.ErrorIs(test, err, workersFailure)
				require.EqualValues(test, 1, sinkCloses.Load(), "the init service sink must already be closed")
			}
			require.EqualValues(test, 1, ownerCloses.Load())
		})
	}
}

func TestSecondaryWorkerDoesNotCloseSnapshotOperation(test *testing.T) {
	var ownerCloses atomic.Int32
	var sinkCloses atomic.Int32
	loader := newSnapshotOperationTestLoader(mockstorage.NewMockStorage(), func() {
		ownerCloses.Add(1)
	}, &sinkCloses)
	runtime := loader.transfer.Runtime.(*abstract.LocalRuntime)
	runtime.ShardingUpload.JobCount = 2
	runtime.CurrentJob = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.Error(test, loader.UploadTables(ctx, snapshotOperationTestTables(), false))
	require.Zero(test, ownerCloses.Load())
}
