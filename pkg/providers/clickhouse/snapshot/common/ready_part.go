package ch_eo_common

// ReadyPart describes a source piece whose INSERT acknowledgements, UUIDs and
// row counts were checked before its temporary tables were renamed to ready.
// It contains detached table data and has no dependency on the uploader.
type ReadyPart struct {
	tablesByShard    []OwnedTable
	rowCountsByShard []uint64
}

func NewReadyPart() *ReadyPart {
	return &ReadyPart{tablesByShard: nil, rowCountsByShard: nil}
}

func (p *ReadyPart) AddShard(table OwnedTable, rows uint64) {
	p.tablesByShard = append(p.tablesByShard, *CopyOwnedTable(&table))
	p.rowCountsByShard = append(p.rowCountsByShard, rows)
}

func (p ReadyPart) ShardCount() int { return len(p.tablesByShard) }

func (p ReadyPart) Shard(shardIndex int) (OwnedTable, uint64) {
	return *CopyOwnedTable(&p.tablesByShard[shardIndex]), p.rowCountsByShard[shardIndex]
}
