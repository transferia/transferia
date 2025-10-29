package exactlyonce

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/doublecloud/transfer/pkg/sink"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares"
	chModel "github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	"go.ytsaurus.tech/yt/go/schema"
)

func setup(t *testing.T) (*chModel.ChDestination, abstract.AsyncSink) {
	target, err := chrecipe.Target(
		chrecipe.WithDatabase("test"),
		chrecipe.WithInitFile("init.sql"),
	)
	require.NoError(t, err)
	target.WithDefaults()
	target.ExactlyOnce = true
	source := &model.MockSource{}
	source.WithDefaults()
	transfer := helpers.MakeTransfer("test-transfer", source, target, abstract.TransferTypeIncrementOnly)
	sinker, err := sink.MakeAsyncSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), coordinator.NewFakeClient(), middlewares.MakeConfig())
	require.NoError(t, err)
	return target, sinker
}

func TestExactlyOnce(t *testing.T) {
	target, sinker := setup(t)
	err := <-sinker.AsyncPush(tableRange("test", "part-1", 0, 100))
	require.NoError(t, err)
	err = <-sinker.AsyncPush(tableRange("test", "part-1", 0, 120))
	require.NoError(t, err)
	err = <-sinker.AsyncPush(tableRange("test", "part-1", 80, 160))
	require.NoError(t, err)
	err = <-sinker.AsyncPush(tableRange("test", "part-1", 160, 200))
	require.NoError(t, err)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		"test",
		helpers.GetSampleableStorageByModel(t, target),
		10*time.Second,
		200,
	))
}

func TestStaleRecordReading(t *testing.T) {
	target, sinker := setup(t)

	// Step 1: Process the first batch of data
	batch := tableRange("test", "part-1", 0, 100)

	err := <-sinker.AsyncPush(batch) // Write the data, BEFORE, and AFTER records
	require.NoError(t, err)

	// Simulate crash before committing offsets
	require.NoError(t, sinker.Close())

	// Step 2: Restart the sinker (simulate application restart after crash)
	sinker, err = sink.MakeAsyncSink(
		helpers.MakeTransfer("test-transfer", new(model.MockSource), target, abstract.TransferTypeIncrementOnly),
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		middlewares.MakeConfig(),
	)
	require.NoError(t, err)

	// Step 3: Process the same batch again (due to uncommitted offset)
	err = <-sinker.AsyncPush(batch) // Reprocess the same data
	require.NoError(t, err)

	// Step 4: Verify that no duplicates exist in the target
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		"test",
		helpers.GetSampleableStorageByModel(t, target),
		10*time.Second,
		100, // The expected row count should match the original batch size
	))
}

func TestConcurrentProcessing(t *testing.T) {
	target, sinker := setup(t)
	defer sinker.Close()

	// Step 1: Process the first batch of data in a delayed manner
	batch := tableRange("test", "part-1", 0, 100)

	done := make(chan struct{})
	go func() {
		defer close(done)
		err := <-sinker.AsyncPush(batch) // Simulate processing
		require.NoError(t, err)
	}()

	// Simulate a processing hang by introducing a delay
	time.Sleep(2 * time.Second)

	// Step 2: Simulate a rebalance that starts a new sinker instance
	sinker.Close()

	newSinker, err := sink.MakeAsyncSink(
		helpers.MakeTransfer("test-transfer", new(model.MockSource), target, abstract.TransferTypeIncrementOnly),
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		middlewares.MakeConfig(),
	)
	require.NoError(t, err)
	defer newSinker.Close()

	// Step 3: New sinker processes the same batch
	err = <-newSinker.AsyncPush(batch) // Reprocess due to rebalance
	require.NoError(t, err)

	// Wait for the original sinker to finish
	<-done

	// Step 4: Verify that no duplicates exist in the target
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		"test",
		helpers.GetSampleableStorageByModel(t, target),
		10*time.Second,
		100, // The expected row count should match the batch size without duplicates
	))
}

// Test for High Throughput and Concurrency
func TestHighThroughputConcurrency(t *testing.T) {
	target, sinker := setup(t)

	concurrentPushes := 5
	partitionRange := 100

	wg := sync.WaitGroup{}
	for i := 0; i < concurrentPushes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := <-sinker.AsyncPush(tableRange("test2", fmt.Sprintf("part-%d", i), 0, partitionRange))
			require.NoError(t, err)
		}(i)
	}

	for i := 0; i < concurrentPushes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := <-sinker.AsyncPush(tableRange("test2", fmt.Sprintf("part-%d", i), 0, partitionRange))
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		"test2",
		helpers.GetSampleableStorageByModel(t, target),
		20*time.Second,
		uint64(concurrentPushes*partitionRange),
	))
}

var tableSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{ColumnName: "_partition", DataType: schema.TypeString.String(), PrimaryKey: true},
	{ColumnName: "_offset", DataType: schema.TypeUint64.String(), PrimaryKey: true},
	{ColumnName: "test1", DataType: schema.TypeString.String()},
	{ColumnName: "test2", DataType: schema.TypeString.String()},
})

func tableRange(table, partID string, l, r int) []abstract.ChangeItem {
	var res []abstract.ChangeItem
	for i := l; i < r; i++ {
		res = append(res, abstract.ChangeItem{
			LSN:          uint64(i),
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(time.Now().UnixNano()),
			Table:        table,
			PartID:       partID,
			ColumnNames:  []string{"_partition", "_offset", "test1", "test2"},
			ColumnValues: []interface{}{partID, uint64(i), fmt.Sprintf("test1_value_%v", i), fmt.Sprintf("test2_value_%v", i)},
			TableSchema:  tableSchema,
		})
	}
	return res
}
