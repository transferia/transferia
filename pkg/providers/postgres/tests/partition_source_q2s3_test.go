package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	dt_metrics "github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/tests/helpers"
	sourcehelpers "github.com/transferia/transferia/tests/helpers/source"
)

func TestPartitionSourceQueueToS3(t *testing.T) {
	transferID := helpers.GenerateTransferID("TestPartitionSourceQueueToS3")
	src := pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
			pg.SlotID = transferID
			pg.DBTables = []string{"public.q2s3_test"}
		}),
	)

	conn, err := provider_postgres.MakeConnPoolFromSrc(src, logger.Log)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS public.q2s3_test (
			id INT PRIMARY KEY,
			val TEXT
		);
	`)
	require.NoError(t, err)

	require.NoError(t, provider_postgres.CreateReplicationSlot(src))
	defer func() {
		_ = provider_postgres.DropReplicationSlot(src)
	}()

	_, err = conn.Exec(ctx, `INSERT INTO public.q2s3_test (id, val) VALUES (1, 'a'), (2, 'b');`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO public.q2s3_test (id, val) VALUES (3, 'c');`)
	require.NoError(t, err)

	partitionSrc, err := provider_postgres.NewPartitionSource(
		src,
		transferID,
		nil,
		abstract.Partition{Topic: transferID, Partition: 0},
		logger.Log,
		stats.NewSourceStats(dt_metrics.NewRegistry()),
		coordinator.NewFakeClient(),
	)
	require.NoError(t, err)
	defer partitionSrc.Stop()

	batches, err := sourcehelpers.WaitForItemsQueueToS3(partitionSrc, 3, 500*time.Millisecond)
	require.NoError(t, err)

	var items []abstract.ChangeItem
	for _, batch := range batches {
		for _, item := range batch {
			if item.Table == "q2s3_test" && item.IsRowEvent() {
				items = append(items, item)
			}
		}
	}
	require.Len(t, items, 3)

	for _, item := range items {
		require.Equal(t, transferID, item.QueueMessageMeta.TopicName)
		require.Equal(t, 0, item.QueueMessageMeta.PartitionNum)
		require.Equal(t, item.LSN, item.QueueMessageMeta.Offset)
		require.Equal(t, item.Counter, item.QueueMessageMeta.Index)
		require.False(t, item.IsSystemTable())
	}
}
