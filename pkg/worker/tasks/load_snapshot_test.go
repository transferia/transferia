package tasks

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers/fake_sharding_storage"
	mockstorage "github.com/transferia/transferia/tests/helpers/mock_storage"
)

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
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
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
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
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
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
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
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
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
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
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
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
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

	snapshotLoader := NewSnapshotLoader(cp, operationID, transfer, solomon.NewRegistry(nil))
	ctx := context.Background()

	// first run
	go func(inSnapshotLoader *SnapshotLoader) {
		_ = inSnapshotLoader.WaitWorkersInitiated(ctx)
		_ = cp.FinishOperation(operationID, "", "", 0, nil)
		_ = cp.FinishOperation(operationID, "", "", 1, nil)
	}(snapshotLoader)
	err := snapshotLoader.UploadTables(ctx, tables, false)
	require.NoError(t, err)

	// second run
	err = snapshotLoader.UploadTables(ctx, tables, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), mainWorkerRestartedErrorText))
}
