package part_uploader

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
)

func TestShardBatchesPreserveOrderAndReleaseRows(test *testing.T) {
	batches := newShardBatches(32)
	for _, rowCount := range []int{1000, 13, 1500, 0} {
		items := make([]abstract.ChangeItem, rowCount)
		for rowIndex := range items {
			items[rowIndex] = abstract.ChangeItem{ID: uint32(rowIndex), Table: "source", ColumnValues: []any{rowIndex}}
		}
		grouped, err := batches.groupByShard(items, func(item abstract.ChangeItem) sharding.ShardID { return sharding.ShardID(item.ID % 32) })
		require.NoError(test, err)
		for shardIndex, rows := range grouped {
			for rowIndex := range rows {
				require.Equal(test, items[shardIndex+rowIndex*32], rows[rowIndex])
			}
		}
		batches.releaseRows()
		for _, row := range batches.groupedRows {
			require.Equal(test, abstract.ChangeItem{}, row)
		}
		for _, rows := range batches.rowsByShard {
			require.Nil(test, rows)
		}
	}
}

func TestShardBatchesRejectInvalidShard(test *testing.T) {
	batches := newShardBatches(32)
	_, err := batches.groupByShard([]abstract.ChangeItem{{}}, func(abstract.ChangeItem) sharding.ShardID { return 32 })
	require.ErrorContains(test, err, "invalid ClickHouse shard 32")
	batches.releaseRows()
}

func BenchmarkShardBatches(bench *testing.B) {
	for _, rowCount := range []int{1024, 8192} {
		items := make([]abstract.ChangeItem, rowCount)
		for rowIndex := range items {
			items[rowIndex].ID = uint32(rowIndex)
		}
		sharder := func(item abstract.ChangeItem) sharding.ShardID { return sharding.ShardID(item.ID % 32) }
		bench.Run(
			fmt.Sprintf("legacy/%d", rowCount),
			func(bench *testing.B) {
				bench.ReportAllocs()
				for iteration := 0; iteration < bench.N; iteration++ {
					rowsByShard := make(map[int][]abstract.ChangeItem)
					for _, item := range items {
						shardIndex := int(sharder(item))
						rowsByShard[shardIndex] = append(rowsByShard[shardIndex], item)
					}
					if len(rowsByShard) != 32 {
						bench.Fatal("missing shard")
					}
				}
			},
		)
		bench.Run(
			fmt.Sprintf("reused/%d", rowCount),
			func(bench *testing.B) {
				batches := newShardBatches(32)
				if _, err := batches.groupByShard(items, sharder); err != nil {
					bench.Fatal(err)
				}
				batches.releaseRows()
				bench.ReportAllocs()
				bench.ResetTimer()
				for iteration := 0; iteration < bench.N; iteration++ {
					if _, err := batches.groupByShard(items, sharder); err != nil {
						bench.Fatal(err)
					}
					batches.releaseRows()
				}
			},
		)
	}
}
