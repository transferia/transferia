package part_uploader

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
)

// shardBatches groups one Push in reusable storage. It never retains source
// values after Push returns and does not allocate a growing slice per shard.
type shardBatches struct {
	groupedRows     []abstract.ChangeItem
	shardIndexByRow []int
	rowsByShard     [][]abstract.ChangeItem
	shardOffsets    []int
}

func newShardBatches(shardCount int) *shardBatches {
	return &shardBatches{
		groupedRows:     nil,
		shardIndexByRow: nil,
		rowsByShard:     make([][]abstract.ChangeItem, shardCount),
		shardOffsets:    make([]int, shardCount+1),
	}
}

func (b *shardBatches) groupByShard(items []abstract.ChangeItem, sharder sharding.Sharder) ([][]abstract.ChangeItem, error) {
	if cap(b.groupedRows) < len(items) {
		b.groupedRows = make([]abstract.ChangeItem, len(items))
		b.shardIndexByRow = make([]int, len(items))
	}
	b.groupedRows = b.groupedRows[:len(items)]
	b.shardIndexByRow = b.shardIndexByRow[:len(items)]
	clear(b.shardOffsets)
	for rowIndex := range items {
		shardIndex := int(sharder(items[rowIndex]))
		if shardIndex < 0 || shardIndex >= len(b.rowsByShard) {
			return nil, xerrors.Errorf("invalid ClickHouse shard %d", shardIndex)
		}
		b.shardIndexByRow[rowIndex] = shardIndex
		b.shardOffsets[shardIndex+1]++
	}
	for shardIndex := range b.rowsByShard {
		b.shardOffsets[shardIndex+1] += b.shardOffsets[shardIndex]
		b.rowsByShard[shardIndex] = b.groupedRows[b.shardOffsets[shardIndex]:b.shardOffsets[shardIndex+1]]
	}
	for rowIndex := range items {
		shardIndex := b.shardIndexByRow[rowIndex]
		b.groupedRows[b.shardOffsets[shardIndex]] = items[rowIndex]
		b.shardOffsets[shardIndex]++
	}
	return b.rowsByShard, nil
}

func (b *shardBatches) releaseRows() {
	clear(b.groupedRows)
	clear(b.rowsByShard)
	// Do not keep an unusually large Push alive for the remainder of a snapshot.
	// Normal batches reuse these arrays; memory never grows with part count.
	if cap(b.groupedRows) > 64*1024 {
		b.groupedRows = nil
		b.shardIndexByRow = nil
	}
}
