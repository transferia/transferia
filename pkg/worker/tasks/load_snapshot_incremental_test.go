package tasks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func TestMergeWithIncrementalState(t *testing.T) {
	transfer := &model.Transfer{
		ID:   "transfer1",
		Type: abstract.TransferTypeSnapshotOnly,
		RegularSnapshot: &abstract.RegularSnapshot{
			Incremental: []abstract.IncrementalTable{
				{Namespace: "public", Name: "table1", CursorField: "field1", InitialState: "100500"},
				{Namespace: "public", Name: "table2", CursorField: "field1", InitialState: "100500"}, // new table
			},
		}}
	client := coordinator.NewFakeClientWithOpts(func(id string) (map[string]*coordinator.TransferStateData, error) {
		result := map[string]*coordinator.TransferStateData{}
		if id == "transfer1" {
			result[TablesFilterStateKey] = &coordinator.TransferStateData{
				IncrementalTables: []abstract.TableDescription{
					{Name: "table1", Schema: "public", Filter: "\"field1\" > 200500"},
				},
			}
		}
		return result, nil
	})
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "public"},
		{Name: "table2", Schema: "public"},
	}
	incrementalStorage := newFakeIncrementalStorage()
	snapshotLoader := NewSnapshotLoader(client, "test-operation", transfer, solomon.NewRegistry(nil))
	outTables, err := snapshotLoader.getIncrementalStateAndMergeWithTables(tables, incrementalStorage)
	require.NoError(t, err)
	require.Equal(t, []abstract.TableDescription{
		{Name: "table1", Schema: "public", Filter: "\"field1\" > 200500"},
		{Name: "table2", Schema: "public", Filter: "\"field1\" > 100500"},
	}, outTables)
}

type fakeIncrementalStorage struct {
}

func newFakeIncrementalStorage() *fakeIncrementalStorage {
	return &fakeIncrementalStorage{}
}

func (f *fakeIncrementalStorage) GetNextIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.IncrementalState, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fakeIncrementalStorage) BuildArrTableDescriptionWithIncrementalState(tables []abstract.TableDescription, incremental []abstract.IncrementalTable) []abstract.TableDescription {
	return postgres.SetInitialState(tables, incremental)
}
