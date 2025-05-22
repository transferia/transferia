package clickhouse

import (
	"sort"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

type Table struct {
	tableID    abstract.TableID
	iter       int
	parts      []TablePart
	partFilter map[string]bool
	shards     map[string][]*clickhouse.Host
	shardNum   map[string]int
	rowFilter  abstract.WhereStatement
}

func (s *Table) Name() string {
	return s.tableID.Fqtn()
}

func (s *Table) FullName() string {
	return s.tableID.Fqtn()
}

func (s *Table) Next() bool {
	s.iter++
	if s.iter >= len(s.parts) {
		return false
	}
	if !s.partFilter[s.parts[s.iter].Key()] {
		return s.Next()
	}
	return true
}

func (s *Table) Err() error {
	return nil
}

func (s *Table) Close() {
}

func (s *Table) Part() (base.DataObjectPart, error) {
	part := s.parts[s.iter]
	shardNum := s.shardNum[part.Shard]
	if shardNum == 0 {
		shardNum = 1 // shard num always start with 1, even if single sharded
	}
	ts := NewTablePart(
		part.ShardCount,
		shardNum,
		part.Shard,
		s.tableID,
		part,
		s.rowFilter,
	)
	return ts, nil
}

func (s *Table) ToOldTableID() (*abstract.TableID, error) {
	return &s.tableID, nil
}

func NewTable(tableID abstract.TableID, parts []TablePart, shards map[string][]*clickhouse.Host, rowFilter abstract.WhereStatement) *Table {
	partFilter := map[string]bool{}
	for _, part := range parts {
		partFilter[part.Key()] = true
	}
	var shardNames []string
	for shard := range shards {
		shardNames = append(shardNames, shard)
	}
	sort.Strings(shardNames)
	shardNum := map[string]int{}
	for i, k := range shardNames {
		shardNum[k] = i + 1 // clickhouse iterate shards from 1 not 0
	}
	return &Table{
		tableID:    tableID,
		parts:      parts,
		iter:       -1,
		partFilter: partFilter,
		rowFilter:  rowFilter,
		shards:     shards,
		shardNum:   shardNum,
	}
}
