package tasks

import (
	"context"
	stderrors "errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	transfererrors "github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/worker/tasks/table_part_provider"
	"github.com/transferia/transferia/pkg/worker/tasks/table_part_provider/shared_memory"
	"github.com/transferia/transferia/tests/helpers/fake_sharding_storage"
	mockstorage "github.com/transferia/transferia/tests/helpers/mock_storage"
)

type countingSlotKiller struct {
	calls int
}

func (k *countingSlotKiller) KillSlot() error {
	k.calls++
	return nil
}

func TestWaitForSlotWaitsAfterMonitorCloses(t *testing.T) {
	monitor := make(chan error)
	close(monitor)
	loader := &SnapshotLoader{slotKillerErrorChannel: monitor, slotKiller: &countingSlotKiller{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- loader.waitForSlot(ctx, cancel)
	}()
	select {
	case err := <-done:
		t.Fatalf("slot monitor returned before upload completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	require.NoError(t, <-done)
}

func TestWaitForSlotError(t *testing.T) {
	slotFailure := stderrors.New("slot failed")
	monitor := make(chan error, 2)
	monitor <- nil
	monitor <- slotFailure
	killer := &countingSlotKiller{}
	loader := &SnapshotLoader{slotKillerErrorChannel: monitor, slotKiller: killer}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := loader.waitForSlot(ctx, cancel)
	require.ErrorIs(t, err, slotFailure)
	var categorized transfererrors.Categorized
	require.ErrorAs(t, err, &categorized)
	require.Equal(t, categories.Source, categorized.Category())
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, 1, killer.calls)
}

func TestWaitForSlotExternalCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	loader := &SnapshotLoader{slotKiller: &countingSlotKiller{}}
	cancel()
	require.NoError(t, loader.waitForSlot(ctx, cancel))
}

func TestWaitWorkersInitiatedAfterCompletion(t *testing.T) {
	const operationID = "completed-workers"
	cp := coordinator.NewStatefulFakeClient()
	require.NoError(t, cp.CreateOperationWorkers(operationID, 2))
	require.NoError(t, cp.FinishOperation(operationID, "", "", 1, nil))
	require.NoError(t, cp.FinishOperation(operationID, "", "", 2, nil))

	loader := &SnapshotLoader{cp: cp, operation: &model.TransferOperation{OperationID: operationID}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, loader.WaitWorkersInitiated(ctx))
}

func TestCheckIncludeDirectives_DataObjects_NoError(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{
		"schema1.table1",
		"schema2.*",
	}}
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table2",
		"schema3.*",
	}} // must be ignored
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
		{Name: "table1", Schema: "schema2"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, &model.TransferOperation{}, transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables, func() (abstract.Storage, error) { return mockstorage.NewMockStorage(), nil })
	require.NoError(t, err)
}

func TestCheckIncludeDirectives_DataObjects_Error(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table3",
		"schema3.*",
	}} // must be ignored
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, &model.TransferOperation{}, transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables, func() (abstract.Storage, error) { return mockstorage.NewMockStorage(), nil })
	require.Error(t, err)
	require.Equal(t, "some tables from include list are missing in the source database: [schema1.table2 schema2.*]", err.Error())
}

func TestCheckIncludeDirectives_DataObjects_FqtnVariants(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{
		"schema1.table1",
		"\"schema1\".table1",
		"schema1.\"table1\"",
		"\"schema1\".\"table1\"",
		"schema2.*",
		"\"schema2\".*",
	}}
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
		{Name: "table1", Schema: "schema2"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, &model.TransferOperation{}, transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables, func() (abstract.Storage, error) { return mockstorage.NewMockStorage(), nil })
	require.NoError(t, err)
}

func TestCheckIncludeDirectives_Src_NoError(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema2.*",
	}}
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
		{Name: "table1", Schema: "schema2"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, &model.TransferOperation{}, transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables, func() (abstract.Storage, error) { return mockstorage.NewMockStorage(), nil })
	require.NoError(t, err)
}

func TestCheckIncludeDirectives_Src_Error(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, &model.TransferOperation{}, transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables, func() (abstract.Storage, error) { return mockstorage.NewMockStorage(), nil })
	require.Error(t, err)
	require.Equal(t, "some tables from include list are missing in the source database: [schema1.table2 schema2.*]", err.Error())
}

func TestDoUploadTables_CtxCancelledNoErr(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}

	storage := mockstorage.NewMockStorage()
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, &model.TransferOperation{}, transfer, solomon.NewRegistry(nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tablesMap, err := storage.TableList(transfer)
	require.NoError(t, err)

	tppGetter, _, err := snapshotLoader.BuildTPP(
		context.Background(),
		logger.Log,
		storage,
		tablesMap.ConvertToTableDescriptions(),
		abstract.WorkerTypeSingleWorker,
	)
	require.NoError(t, err)

	err = snapshotLoader.DoUploadTables(ctx, storage, tppGetter)
	require.NoError(t, err)
}

func TestMainWorkerRestart(t *testing.T) {
	metaCheckInterval = 100 * time.Millisecond

	tables := []abstract.TableDescription{{Schema: "schema1", Name: "table1"}}
	operationID := "dtj"
	task := &model.TransferOperation{OperationID: operationID}

	transfer := &model.Transfer{
		Runtime: &abstract.LocalRuntime{ShardingUpload: abstract.ShardUploadParams{JobCount: 2, ProcessCount: 1}},
		Src: &model.MockSource{
			StorageFactory: func() abstract.Storage {
				return fake_sharding_storage.NewFakeShardingStorage(tables)
			},
			AllTablesFactory: func() abstract.TableMap {
				return nil
			},
		},
		Dst: &model.MockDestination{
			SinkerFactory: func() abstract.Sinker {
				return newFakeSink(func(items []abstract.ChangeItem) error {
					return nil
				})
			},
		},
	}

	cp := coordinator.NewStatefulFakeClient()

	snapshotLoader := NewSnapshotLoader(cp, task, transfer, solomon.NewRegistry(nil))
	ctx := context.Background()

	// first run
	go func(inSnapshotLoader *SnapshotLoader) {
		_ = inSnapshotLoader.WaitWorkersInitiated(ctx)
		_ = cp.FinishOperation(operationID, "", "", 1, nil)
		_ = cp.FinishOperation(operationID, "", "", 2, nil)
	}(snapshotLoader)
	err := snapshotLoader.UploadTables(ctx, tables, false)
	require.NoError(t, err)

	// second run
	err = snapshotLoader.UploadTables(ctx, tables, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), mainWorkerRestartedErrorText))
}

func TestMainWorkerSlotError(t *testing.T) {
	tables := []abstract.TableDescription{{Schema: "schema1", Name: "table1"}}
	transfer := &model.Transfer{
		Runtime: &abstract.LocalRuntime{ShardingUpload: abstract.ShardUploadParams{JobCount: 2, ProcessCount: 1}},
		Src: &model.MockSource{StorageFactory: func() abstract.Storage {
			return fake_sharding_storage.NewFakeShardingStorage(tables)
		}},
		Dst: &model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return newFakeSink(func([]abstract.ChangeItem) error { return nil })
		}},
	}
	loader := NewSnapshotLoader(coordinator.NewStatefulFakeClient(), &model.TransferOperation{OperationID: "slot-error"}, transfer, solomon.NewRegistry(nil))
	slotFailure := stderrors.New("slot failed")
	monitor := make(chan error, 1)
	monitor <- slotFailure
	loader.slotKillerErrorChannel = monitor
	killer := &countingSlotKiller{}
	loader.slotKiller = killer

	err := loader.UploadTables(context.Background(), tables, false)
	require.ErrorIs(t, err, slotFailure)
	require.Equal(t, 1, killer.calls)
}

func TestSingleWorkerUploadCancelled(t *testing.T) {
	tables := []abstract.TableDescription{{Schema: "schema1", Name: "table1"}}
	loadStarted := make(chan struct{}, 1)
	storage := mockstorage.NewMockStorage()
	storage.LoadTableF = func(ctx context.Context, _ abstract.TableDescription, _ abstract.Pusher) error {
		select {
		case loadStarted <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	var kindsMu sync.Mutex
	var kinds []abstract.Kind
	transfer := &model.Transfer{
		Src: &model.MockSource{StorageFactory: func() abstract.Storage { return storage }},
		Dst: &model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return newFakeSink(func(items []abstract.ChangeItem) error {
				kindsMu.Lock()
				defer kindsMu.Unlock()
				for _, item := range items {
					kinds = append(kinds, item.Kind)
				}
				return nil
			})
		}},
	}
	loader := NewSnapshotLoader(coordinator.NewStatefulFakeClient(), &model.TransferOperation{OperationID: "cancelled"}, transfer, solomon.NewRegistry(nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-loadStarted
		cancel()
	}()

	err := loader.UploadTables(ctx, tables, false)
	require.ErrorIs(t, err, context.Canceled)
	kindsMu.Lock()
	defer kindsMu.Unlock()
	require.Contains(t, kinds, abstract.InitShardedTableLoad)
	require.NotContains(t, kinds, abstract.DoneShardedTableLoad)
}

type uploadTablePartGetter struct {
	memory abstract.SharedMemory
	next   func(context.Context) (*abstract.OperationTablePart, error)
}

var _ table_part_provider.AbstractTablePartProviderGetter = (*uploadTablePartGetter)(nil)

func (g *uploadTablePartGetter) SharedMemory() abstract.SharedMemory {
	return g.memory
}

func (g *uploadTablePartGetter) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	return g.next(ctx)
}

func newUploadTablesTestLoader(storage abstract.Storage, parallelism int) *SnapshotLoader {
	transfer := &model.Transfer{
		Runtime: &abstract.LocalRuntime{ShardingUpload: abstract.ShardUploadParams{ProcessCount: parallelism}},
		Src:     &model.MockSource{StorageFactory: func() abstract.Storage { return storage }},
		Dst: &model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return newFakeSink(func([]abstract.ChangeItem) error { return nil })
		}},
	}
	return NewSnapshotLoader(&FakeControlplane{}, &model.TransferOperation{}, transfer, solomon.NewRegistry(nil))
}

func TestDoUploadTablesParallelism(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan string, 4)
	assigned := make(chan string, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var active, maximum atomic.Int64
	storage := mockstorage.NewMockStorage()
	storage.LoadTableF = func(ctx context.Context, table abstract.TableDescription, _ abstract.Pusher) error {
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, current) {
				break
			}
		}
		started <- table.Name
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	parts := abstract.NewOperationTablePartFromDescriptionArr("",
		abstract.TableDescription{Name: "first"}, abstract.TableDescription{Name: "second"},
		abstract.TableDescription{Name: "third"}, abstract.TableDescription{Name: "fourth"})
	expectedInitial := []string{parts[0].Name, parts[1].Name}
	expectedRemaining := []string{parts[2].Name, parts[3].Name}
	var getterActive atomic.Int64
	var getterOverlap atomic.Bool
	nilParts := 0
	getter := &uploadTablePartGetter{memory: shared_memory.NewLocal(""), next: func(context.Context) (*abstract.OperationTablePart, error) {
		if getterActive.Add(1) != 1 {
			getterOverlap.Store(true)
		}
		defer getterActive.Add(-1)
		runtime.Gosched()
		if len(parts) == 0 {
			nilParts++
			return nil, nil
		}
		part := parts[0]
		parts = parts[1:]
		assigned <- part.Name
		return part, nil
	}}
	loader := newUploadTablesTestLoader(storage, 2)
	done := make(chan error, 1)
	go func() { done <- loader.DoUploadTables(ctx, storage, getter) }()
	initialLoads := make([]string, 0, 2)
	for range 2 {
		select {
		case name := <-started:
			initialLoads = append(initialLoads, name)
		case <-ctx.Done():
			t.Fatal("uploads did not start")
		}
	}
	initialAssignments := make([]string, 0, 2)
	for range 2 {
		select {
		case name := <-assigned:
			initialAssignments = append(initialAssignments, name)
		case <-ctx.Done():
			t.Fatal("next part was not assigned")
		}
	}
	select {
	case name := <-started:
		t.Fatalf("upload %s started above parallelism limit", name)
	default:
	}
	select {
	case name := <-assigned:
		t.Fatalf("part %s assigned before a worker became available", name)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("uploads did not complete")
	}
	require.ElementsMatch(t, expectedInitial, initialLoads)
	require.ElementsMatch(t, expectedInitial, initialAssignments)
	require.EqualValues(t, 2, maximum.Load())
	require.Len(t, started, 2)
	remainingLoads := []string{<-started, <-started}
	remainingAssignments := []string{<-assigned, <-assigned}
	require.ElementsMatch(t, expectedRemaining, remainingLoads)
	require.ElementsMatch(t, expectedRemaining, remainingAssignments)
	require.False(t, getterOverlap.Load(), "getter must be called serially")
	require.Equal(t, 1, nilParts, "getter must not be called again after exhaustion")
}

func TestDoUploadTablesGetterErrorCancelsAndWaits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	storage := mockstorage.NewMockStorage()
	storage.LoadTableF = func(ctx context.Context, _ abstract.TableDescription, _ abstract.Pusher) error {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return ctx.Err()
	}
	getterErr := stderrors.New("getter failed")
	calls := 0
	getter := &uploadTablePartGetter{memory: shared_memory.NewLocal(""), next: func(ctx context.Context) (*abstract.OperationTablePart, error) {
		calls++
		if calls == 1 {
			return &abstract.OperationTablePart{Name: "first"}, nil
		}
		select {
		case <-started:
			return nil, getterErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	loader := newUploadTablesTestLoader(storage, 2)
	done := make(chan error, 1)
	go func() { done <- loader.DoUploadTables(ctx, storage, getter) }()
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("getter failure did not cancel upload")
	}
	select {
	case err := <-done:
		t.Fatalf("returned before active upload exited: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		require.ErrorIs(t, err, getterErr)
	case <-ctx.Done():
		t.Fatal("did not return after active upload exited")
	}
}

func TestDoUploadTablesCancellationStopsAssigningParts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	var loads atomic.Int64
	storage := mockstorage.NewMockStorage()
	storage.LoadTableF = func(ctx context.Context, _ abstract.TableDescription, _ abstract.Pusher) error {
		loads.Add(1)
		close(started)
		<-ctx.Done()
		return nil
	}
	calls := 0
	getter := &uploadTablePartGetter{memory: shared_memory.NewLocal(""), next: func(context.Context) (*abstract.OperationTablePart, error) {
		calls++
		return &abstract.OperationTablePart{Name: "table"}, nil
	}}
	loader := newUploadTablesTestLoader(storage, 1)
	done := make(chan error, 1)
	go func() { done <- loader.DoUploadTables(ctx, storage, getter) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("upload did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("uploads did not stop")
	}
	require.Equal(t, 1, calls)
	require.EqualValues(t, 1, loads.Load())
}

func TestDoUploadTablesKeepsUploadError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	uploadErr := stderrors.New("source failed")
	var loads atomic.Int64
	storage := mockstorage.NewMockStorage()
	storage.LoadTableF = func(context.Context, abstract.TableDescription, abstract.Pusher) error {
		loads.Add(1)
		return abstract.NewFatalError(uploadErr)
	}
	calls := 0
	getter := &uploadTablePartGetter{memory: shared_memory.NewLocal(""), next: func(ctx context.Context) (*abstract.OperationTablePart, error) {
		calls++
		if calls == 1 {
			return &abstract.OperationTablePart{Name: "first"}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	loader := newUploadTablesTestLoader(storage, 2)
	err := loader.DoUploadTables(ctx, storage, getter)
	require.ErrorIs(t, err, uploadErr)
	require.True(t, abstract.IsFatal(err))
	var categorized transfererrors.Categorized
	require.ErrorAs(t, err, &categorized)
	require.Equal(t, categories.Source, categorized.Category())
	require.EqualValues(t, 1, loads.Load())
}

func TestDoUploadTablesKeepsFirstGetterError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	getterErr := stderrors.New("getter failed first")
	uploadErr := stderrors.New("source failed after cancellation")
	storage := mockstorage.NewMockStorage()
	storage.LoadTableF = func(ctx context.Context, _ abstract.TableDescription, _ abstract.Pusher) error {
		close(started)
		<-ctx.Done()
		return abstract.NewFatalError(uploadErr)
	}
	calls := 0
	getter := &uploadTablePartGetter{memory: shared_memory.NewLocal(""), next: func(ctx context.Context) (*abstract.OperationTablePart, error) {
		calls++
		if calls == 1 {
			return &abstract.OperationTablePart{Name: "first"}, nil
		}
		select {
		case <-started:
			return nil, getterErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	loader := newUploadTablesTestLoader(storage, 2)
	err := loader.DoUploadTables(ctx, storage, getter)
	require.ErrorIs(t, err, getterErr)
	require.NotErrorIs(t, err, uploadErr)
	var categorized transfererrors.Categorized
	require.ErrorAs(t, err, &categorized)
	require.Equal(t, categories.Internal, categorized.Category())
}

func TestDoUploadTablesWrappedCancellationNoErr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	getter := &uploadTablePartGetter{memory: shared_memory.NewLocal(""), next: func(ctx context.Context) (*abstract.OperationTablePart, error) {
		close(started)
		<-ctx.Done()
		return nil, fmt.Errorf("getter interrupted: %w", ctx.Err())
	}}
	storage := mockstorage.NewMockStorage()
	loader := newUploadTablesTestLoader(storage, 1)
	done := make(chan error, 1)
	go func() { done <- loader.DoUploadTables(ctx, storage, getter) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("getter did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("getter did not stop after cancellation")
	}
}

func TestDoUploadTablesInternalCancellationReturnsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	getter := &uploadTablePartGetter{memory: shared_memory.NewLocal(""), next: func(context.Context) (*abstract.OperationTablePart, error) {
		return nil, fmt.Errorf("getter interrupted: %w", context.Canceled)
	}}
	storage := mockstorage.NewMockStorage()
	loader := newUploadTablesTestLoader(storage, 1)
	err := loader.DoUploadTables(ctx, storage, getter)
	require.NoError(t, ctx.Err(), "caller context must remain live")
	require.ErrorIs(t, err, context.Canceled)
	var categorized transfererrors.Categorized
	require.ErrorAs(t, err, &categorized)
	require.Equal(t, categories.Internal, categorized.Category())
}

func TestDoUploadTablesCallerDeadlineNoErr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	getter := &uploadTablePartGetter{memory: shared_memory.NewLocal(""), next: func(ctx context.Context) (*abstract.OperationTablePart, error) {
		calls++
		<-ctx.Done()
		return nil, fmt.Errorf("getter interrupted: %w", ctx.Err())
	}}
	storage := mockstorage.NewMockStorage()
	loader := newUploadTablesTestLoader(storage, 1)
	err := loader.DoUploadTables(ctx, storage, getter)
	require.Equal(t, 1, calls, "getter must start before caller deadline")
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	require.NoError(t, err)
}
