package tasks

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util/set"
)

type FakeControlplane struct {
	coordinator.CoordinatorNoOp
	transfer *model.Transfer
}

func (f *FakeControlplane) GetEndpoint(transferID string, isSource bool) (model.EndpointParams, error) {
	if isSource {
		return f.transfer.Src, nil
	}
	return f.transfer.Dst, nil
}

func (f *FakeControlplane) GetEndpointTransfers(transferID string, isSource bool) ([]*model.Transfer, error) {
	result := make([]*model.Transfer, 1)
	result[0] = f.transfer
	return result, nil
}

var (
	table1                = abstract.TableID{Namespace: "public", Name: "table1"}
	table2                = abstract.TableID{Namespace: "public", Name: "table2"}
	waitTable1LoadStarted = sync.WaitGroup{}
)

func TableDescription(id abstract.TableID) abstract.TableDescription {
	description := new(abstract.TableDescription)
	description.Name = id.Name
	description.Schema = id.Namespace
	description.EtaRow = 10
	return *description
}

func InsertRow(table abstract.TableID) abstract.ChangeItem {
	item := new(abstract.ChangeItem)
	item.Schema = table.Namespace
	item.Table = table.Name
	item.Kind = abstract.InsertKind
	return *item
}

type testTransformer struct {
	mu    sync.Mutex
	stats map[abstract.TableID]uint64
}

func (t *testTransformer) Type() abstract.TransformerType {
	return abstract.TransformerType("test_transformer")
}

func (t *testTransformer) Apply(items []abstract.ChangeItem) abstract.TransformerResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, item := range items {
		if item.IsRowEvent() {
			t.stats[item.TableID()] += 1
		}
	}
	return abstract.TransformerResult{
		Transformed: items,
		Errors:      nil,
	}
}

func (t *testTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return true
}

func (t *testTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *testTransformer) Description() string {
	return "Transformer for tests"
}

func newTestTransformer() *testTransformer {
	return &testTransformer{
		stats: make(map[abstract.TableID]uint64),
	}
}

type mockStorage struct{}

func (m *mockStorage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return nil, nil
}

func (m *mockStorage) Close() {
}

func (m *mockStorage) Ping() error {
	return nil
}

func (m *mockStorage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	// guarantee parallel tables loading
	if table.ID() != table1 {
		waitTable1LoadStarted.Wait()
	}
	for i := uint64(0); i < table.EtaRow; i++ {
		err := pusher([]abstract.ChangeItem{InsertRow(table.ID())})
		if err != nil {
			return abstract.NewFatalError(err)
		}
		if table.ID() == table1 && i == 0 {
			waitTable1LoadStarted.Done()
		}
	}

	return nil
}

func (m *mockStorage) TableList(abstract.IncludeTableList) (abstract.TableMap, error) {
	return nil, nil
}

func (m *mockStorage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, xerrors.New("not implemented")
}

func (m *mockStorage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, xerrors.New("not implemented")
}

func (m *mockStorage) TableExists(table abstract.TableID) (bool, error) {
	return false, xerrors.New("not implemented")
}

type mockSinker struct {
	excludedKinds *set.Set[abstract.Kind]
	mux           sync.Mutex
	table         abstract.TableID
}

func newMockSinker(excludedKinds ...abstract.Kind) *mockSinker {
	return &mockSinker{
		excludedKinds: set.New(excludedKinds...),
		mux:           sync.Mutex{},
		table:         abstract.TableID{},
	}
}

func (m *mockSinker) Close() error {
	return nil
}

func (m *mockSinker) checkSameTable(table abstract.TableID) bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.table.Name == "" && m.table.Namespace == "" {
		m.table = table
		return true
	}
	return m.table == table
}

func (m *mockSinker) Push(input []abstract.ChangeItem) error {
	for _, item := range input {
		if m.excludedKinds.Contains(item.Kind) {
			continue
		}
		inputTableID := item.TableID()
		if !m.checkSameTable(inputTableID) {
			return xerrors.Errorf("try to push foreign items(%v) into sinker(%v)", inputTableID.Fqtn(), m.table.Fqtn())
		}
	}
	return nil
}

func TestEveryTableHasOwnSink(t *testing.T) {
	transfer := &model.Transfer{
		ID: "test_transfer_id",
		Runtime: &abstract.LocalRuntime{
			ShardingUpload: abstract.ShardUploadParams{
				ProcessCount: 2,
			},
		},
		Src: &model.MockSource{
			StorageFactory:   func() abstract.Storage { return &mockStorage{} },
			AllTablesFactory: func() abstract.TableMap { return nil },
		},
		Dst: &model.MockDestination{
			SinkerFactory: func() abstract.Sinker {
				return newMockSinker(abstract.InitShardedTableLoad, abstract.DoneShardedTableLoad)
			},
		},
	}

	transformer := newTestTransformer()

	err := transfer.AddExtraTransformer(transformer)
	require.NoError(t, err)

	// need to synchronize load tables in parallel
	waitTable1LoadStarted.Add(1)

	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	err = snapshotLoader.UploadTables(
		context.Background(),
		[]abstract.TableDescription{
			TableDescription(table1),
			TableDescription(table2),
		},
		true,
	)
	require.NoError(t, err)

	require.Equal(t, transformer.stats[table1], TableDescription(table1).EtaRow)
	require.Equal(t, transformer.stats[table2], TableDescription(table2).EtaRow)
}
