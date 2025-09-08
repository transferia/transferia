package table_part_provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestSingleWorkerTPPFullSync(t *testing.T) {
	ctx := context.Background()
	descs := []*abstract.TableDescription{
		{Schema: "schema-1", Name: "table-1", Filter: "a<5"},
		{Schema: "schema-1", Name: "table-1", Filter: "a>5"},
		{Schema: "schema-2", Name: "table-2"},
	}
	parts := make([]*abstract.OperationTablePart, 0)
	for _, desc := range descs {
		parts = append(parts, abstract.NewOperationTablePartFromDescription("dtjtest", desc))
	}

	t.Run("empty synchronous", func(t *testing.T) {
		provider := NewSingleWorkerTPPFullSync()
		require.NotPanics(t, provider.Close)
		require.NotPanics(t, provider.Close) // Check that many Closes won't panic.
		checkPartProvider(t, []*abstract.OperationTablePart{nil}, provider)
	})

	t.Run("synchronous", func(t *testing.T) {
		tablePartProviderFull := NewSingleWorkerTPPFullSync()
		err := tablePartProviderFull.AppendParts(ctx, parts)
		require.NoError(t, err)
		require.NotPanics(t, tablePartProviderFull.Close)
		require.NotPanics(t, tablePartProviderFull.Close) // Check that many Closes won't panic.
		checkPartProvider(t, append(parts, nil), tablePartProviderFull)
	})
}
