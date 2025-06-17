//go:build !disable_clickhouse_provider

package clickhouse

import (
	"encoding/json"
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type TablePartA2 struct {
	Shard      string
	TableID    abstract.TableID
	ShardNum   int
	ShardCount int
	Part       TablePart
	RowFilter  abstract.WhereStatement
}

func (t *TablePartA2) Name() string {
	return t.TableID.Fqtn()
}

func (t *TablePartA2) FullName() string {
	return fmt.Sprintf("%v/%v(%v)", t.Shard, t.TableID.Fqtn(), t.Part.Name)
}

func (t *TablePartA2) ToOldTableDescription() (*abstract.TableDescription, error) {
	filter := abstract.NoFilter
	if t.Part.Name != "" {
		filter = abstract.FiltersIntersection(filter, abstract.WhereStatement(fmt.Sprintf("_partition_id = '%s'", t.Part.Name)))
	}
	if t.RowFilter != "" {
		filter = abstract.FiltersIntersection(filter, t.RowFilter)
	}
	if t.ShardCount > 1 {
		filter = abstract.FiltersIntersection(filter, abstract.WhereStatement(fmt.Sprintf("'%[1]s' = '%[1]s'", t.Shard)))
	}
	return &abstract.TableDescription{
		Name:   t.TableID.Name,
		Schema: t.TableID.Namespace,
		Filter: filter,
		EtaRow: uint64(t.Part.Rows),
		Offset: 0,
	}, nil
}

func (t *TablePartA2) ToTablePart() (*abstract.TableDescription, error) {
	serializedPart, err := json.Marshal(t)
	if err != nil {
		return nil, xerrors.Errorf("Can't serialize table part: %w", err)
	}

	return &abstract.TableDescription{
		Name:   t.TableID.Name,
		Schema: t.TableID.Namespace,
		Filter: abstract.WhereStatement(serializedPart),
		EtaRow: uint64(t.Part.Rows),
		Offset: 0,
	}, nil
}

func NewTablePart(shardCount int, shardNum int, shardName string, tID abstract.TableID, part TablePart, rowFilter abstract.WhereStatement) *TablePartA2 {
	return &TablePartA2{
		Shard:      shardName,
		TableID:    tID,
		ShardNum:   shardNum,
		ShardCount: shardCount,
		Part:       part,
		RowFilter:  rowFilter,
	}
}
