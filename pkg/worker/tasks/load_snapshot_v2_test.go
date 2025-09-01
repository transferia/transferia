package tasks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/stdout"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestSnapshotLoader_doUploadTablesV2(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}

	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := snapshotLoader.doUploadTablesV2(ctx, nil, NewLocalTablePartProvider().TablePartProvider())
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
	ctx := context.Background()

	providers.Register(abstract.ProviderTypeMock, newAbstract2Provider)

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

	cp := coordinator.NewFakeClientWithOpts(
		nil,
		func() ([]*model.OperationWorker, error) {
			return []*model.OperationWorker{
				{Completed: true},
				{Completed: true},
			}, nil
		},
		func(operationID string) (*model.AggregatedProgress, error) {
			return &model.AggregatedProgress{}, nil
		},
	)
	snapshotLoader := NewSnapshotLoader(cp, "test-operation", transfer, solomon.NewRegistry(nil))
	var err error
	err = snapshotLoader.uploadV2Main(ctx, nil, []abstract.TableDescription{{Schema: "schema", Name: "name"}})
	require.NoError(t, err)
	err = snapshotLoader.uploadV2Single(ctx, nil, []abstract.TableDescription{{Schema: "schema", Name: "name"}})
	require.NoError(t, err)

	err = ActivateDelivery(ctx, nil, cp, *transfer, solomon.NewRegistry(nil))
	require.NoError(t, err)
}
