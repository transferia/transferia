package part_uploader

import ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"

type shard struct {
	table        ch_eo_common.OwnedTable
	expectedRows uint64
}

func newShard(table ch_eo_common.OwnedTable, rows uint64) shard {
	return shard{table: table, expectedRows: rows}
}
