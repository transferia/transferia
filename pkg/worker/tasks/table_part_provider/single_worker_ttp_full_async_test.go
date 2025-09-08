package table_part_provider

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestSingleWorkerTPPFullAsync(t *testing.T) {
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

	t.Run("asynchronous", func(t *testing.T) {
		provider := NewSingleWorkerTPPFullAsync()

		require.NoError(t, provider.AppendParts(ctx, parts[:2]))
		checkPartProvider(t, parts[:2], provider)

		require.NoError(t, provider.AppendParts(ctx, []*abstract.OperationTablePart{parts[2]}))
		checkPartProvider(t, []*abstract.OperationTablePart{parts[2]}, provider)

		// Check that cancellation of context won't cause deadlock.
		provider.partsCh = make(chan *abstract.OperationTablePart, 1) // Use 1 to make channel filled.
		ctx, cancel := context.WithCancel(ctx)
		waitCh := make(chan struct{})
		go func() {
			defer close(waitCh)
			// Will be cancelled (bcs len(parts) > cap(chan)), so err is not nil.
			require.Error(t, provider.AppendParts(ctx, parts))
		}()
		time.Sleep(50 * time.Millisecond)
		cancel()
		<-waitCh

		require.NotPanics(t, provider.Close)
		require.NotPanics(t, provider.Close) // Check that many Closes won't panic.
		require.Error(t, provider.AppendParts(ctx, parts))
	})
}
