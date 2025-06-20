//go:build !disable_yt_provider

package dataobjects

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"go.ytsaurus.tech/yt/go/yt"
)

type preshardedDataObject struct {
	idx      int
	name     string
	partKeys []partKey
	txID     yt.TxID
}

func (o *preshardedDataObject) Name() string {
	return o.name
}

func (o *preshardedDataObject) FullName() string {
	return o.name
}

func (o *preshardedDataObject) Next() bool {
	o.idx++
	return o.idx < len(o.partKeys)
}

func (o *preshardedDataObject) Err() error {
	return nil
}

func (o *preshardedDataObject) Close() {
	o.idx = len(o.partKeys)
}

func (o *preshardedDataObject) Part() (base.DataObjectPart, error) {
	if l := len(o.partKeys); o.idx >= l {
		return nil, xerrors.Errorf("part index %d out of range %d", o.idx, l)
	}
	k := o.partKeys[o.idx]
	return NewPart(k.Table, k.NodeID, k.Rng, o.txID), nil
}

func (o *preshardedDataObject) ToOldTableID() (*abstract.TableID, error) {
	return &abstract.TableID{
		Namespace: "",
		Name:      o.Name(),
	}, nil
}

func newPreshardedDataObject(txID yt.TxID, parts []partKey) *preshardedDataObject {
	return &preshardedDataObject{
		idx:      -1,
		name:     parts[0].Table,
		partKeys: parts,
		txID:     txID,
	}
}
