package tasks

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/stdout"
	mockstorage "github.com/transferia/transferia/tests/helpers/mock_storage"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestSnapshotLoader_doUploadTablesV2(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}

	registry := solomon.NewRegistry(nil)
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, registry)

	srcStorage := mockstorage.NewMockStorage()
	defer srcStorage.Close()

	tablesMap, err := srcStorage.TableList(transfer)
	require.NoError(t, err)

	tppGetter, _, err := snapshotLoader.BuildTPP(
		context.Background(),
		logger.Log,
		srcStorage,
		tablesMap.ConvertToTableDescriptions(),
		abstract.WorkerTypeSingleWorker,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = snapshotLoader.doUploadTablesV2(ctx, nil, tppGetter)
	require.NoError(t, err)
}

//---------------------------------------------------------------------------------------------------------------------

type abstract2Provider struct {
}

func (p *abstract2Provider) Type() abstract.ProviderType {
	return abstract.ProviderTypeMock
}

func (p *abstract2Provider) DataProvider() (base.DataProvider, error) {
	return p, nil
}

func (p *abstract2Provider) Init() error {
	return nil
}
func (p *abstract2Provider) Ping() error {
	return nil
}
func (p *abstract2Provider) Close() error {
	return nil
}

func (p *abstract2Provider) BeginSnapshot() error {
	return nil
}
func (p *abstract2Provider) DataObjects(filter base.DataObjectFilter) (base.DataObjects, error) {
	return nil, nil
}
func (p *abstract2Provider) TableSchema(part base.DataObjectPart) (*abstract.TableSchema, error) {
	return nil, nil
}
func (p *abstract2Provider) CreateSnapshotSource(part base.DataObjectPart) (base.ProgressableEventSource, error) {
	return nil, nil
}
func (p *abstract2Provider) EndSnapshot() error {
	return nil
}
func (p *abstract2Provider) ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (base.DataObjectPart, error) {
	return nil, nil
}
func (p *abstract2Provider) DataObjectsToTableParts(filter base.DataObjectFilter) ([]abstract.TableDescription, error) {
	return nil, nil
}
func (p *abstract2Provider) TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (base.DataObjectPart, error) {
	return nil, nil
}

func newAbstract2Provider(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return &abstract2Provider{}
}

func TestAbstract2SourcePassViaWholeUploadPipeline(t *testing.T) {
	metaCheckInterval = 100 * time.Millisecond
	providers.Register(abstract.ProviderTypeMock, newAbstract2Provider)

	ctx := context.Background()

	transfer := new(model.Transfer)
	transfer.Src = &model.MockSource{
		IsAbstract2Val: true,
	}
	transfer.Dst = &stdout.StdoutDestination{}
	transfer.Runtime = &abstract.LocalRuntime{
		ShardingUpload: abstract.ShardUploadParams{
			JobCount: 2,
		},
	}

	cp := coordinator.NewStatefulFakeClient()

	t.Run("uploadV2Main", func(t *testing.T) {
		operationID := "test-operation-1"
		snapshotLoader := NewSnapshotLoader(cp, operationID, transfer, solomon.NewRegistry(nil))

		go func(inSnapshotLoader *SnapshotLoader) {
			_ = inSnapshotLoader.WaitWorkersInitiated(ctx)
			_ = cp.FinishOperation(operationID, "", "", 1, nil)
			_ = cp.FinishOperation(operationID, "", "", 2, nil)
		}(snapshotLoader)

		err := snapshotLoader.uploadV2Main(ctx, nil, []abstract.TableDescription{{Schema: "schema", Name: "name"}})
		require.NoError(t, err)
	})

	t.Run("uploadV2Single", func(t *testing.T) {
		operationID := "test-operation-2"
		snapshotLoader := NewSnapshotLoader(cp, operationID, transfer, solomon.NewRegistry(nil))

		err := snapshotLoader.uploadV2Single(ctx, nil, []abstract.TableDescription{{Schema: "schema", Name: "name"}})
		require.NoError(t, err)
	})

	t.Run("ActivateDelivery", func(t *testing.T) {
		operationID := "test-operation-3"
		snapshotLoader := NewSnapshotLoader(cp, operationID, transfer, solomon.NewRegistry(nil))

		go func(inSnapshotLoader *SnapshotLoader) {
			_ = inSnapshotLoader.WaitWorkersInitiated(ctx)
			_ = cp.FinishOperation(operationID, "", "", 1, nil)
			_ = cp.FinishOperation(operationID, "", "", 2, nil)
		}(snapshotLoader)

		task := &model.TransferOperation{
			OperationID: operationID,
		}
		err := ActivateDelivery(ctx, task, cp, *transfer, solomon.NewRegistry(nil))
		require.NoError(t, err)
	})
}

func TestMainWorkerRestartV2(t *testing.T) {
	metaCheckInterval = 100 * time.Millisecond
	providers.Register(abstract.ProviderTypeMock, newAbstract2Provider)

	tables := []abstract.TableDescription{{Schema: "schema1", Name: "table1"}}
	operationID := "dtj"

	transfer := &model.Transfer{
		Runtime: &abstract.LocalRuntime{ShardingUpload: abstract.ShardUploadParams{JobCount: 2, ProcessCount: 1}},
		Src: &model.MockSource{
			IsAbstract2Val: true,
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
		_ = cp.FinishOperation(operationID, "", "", 1, nil)
		_ = cp.FinishOperation(operationID, "", "", 2, nil)
	}(snapshotLoader)
	err := snapshotLoader.UploadV2(ctx, nil, tables)
	require.NoError(t, err)

	// second run
	err = snapshotLoader.UploadV2(ctx, nil, tables)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), mainWorkerRestartedErrorText))
}
