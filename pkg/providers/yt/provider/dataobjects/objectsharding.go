//go:build !disable_yt_provider

package dataobjects

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers/yt/tablemeta"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const MinShardSize = 50000

type shardingDataObject struct {
	idx       int64
	shardSize int64
	table     *tablemeta.YtTableMeta
	txID      yt.TxID
}

func (o *shardingDataObject) Name() string {
	return o.table.Name
}

func (o *shardingDataObject) FullName() string {
	return o.table.FullName()
}

func (o *shardingDataObject) Next() bool {
	o.idx += o.shardSize
	return o.idx < o.table.RowCount
}

func (o *shardingDataObject) Err() error {
	return nil
}

func (o *shardingDataObject) Close() {
	o.idx = o.table.RowCount
}

func (o *shardingDataObject) Part() (base.DataObjectPart, error) {
	if o.idx < 0 && o.idx >= o.table.RowCount {
		return nil, xerrors.Errorf("iter idx %d out of bounds", o.idx)
	}
	return o.part(), nil
}

func (o *shardingDataObject) part() *Part {
	lastIdx := o.idx + o.shardSize
	if lastIdx > o.table.RowCount {
		lastIdx = o.table.RowCount
	}
	r := ypath.Interval(ypath.RowIndex(o.idx), ypath.RowIndex(lastIdx))
	return NewPart(o.table.Name, *o.table.NodeID, r, o.txID)
}

func (o *shardingDataObject) ToOldTableID() (*abstract.TableID, error) {
	return &abstract.TableID{
		Namespace: "",
		Name:      o.Name(),
	}, nil
}

func newShardingDataObject(table *tablemeta.YtTableMeta, txID yt.TxID, shardCount int) *shardingDataObject {
	shardSize := table.RowCount/int64(shardCount) + 1
	if shardSize < MinShardSize {
		shardSize = MinShardSize
	}
	return &shardingDataObject{
		idx:       -shardSize,
		shardSize: shardSize,
		table:     table,
		txID:      txID,
	}
}
