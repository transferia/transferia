package tasks

import (
	"context"
	stderrors "errors"
	"strings"
	"sync"
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
