//go:build !disable_yt_provider

package dataobjects

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const RowIdxKey = "$row_index"

type Part struct {
	name   string
	nodeID yt.NodeID
	rng    ypath.Range
	txID   yt.TxID
}

func (p *Part) Name() string {
	return p.name
}

func (p *Part) FullName() string {
	return p.name
}

func (p *Part) ToOldTableDescription() (*abstract.TableDescription, error) {
	lower := p.LowerBound()
	upper := p.UpperBound()
	return &abstract.TableDescription{
		Name:   p.Name(),
		Schema: "",
		Filter: rangeToLegacyWhere(p.rng),
		EtaRow: upper - lower,
		Offset: lower,
	}, nil
}

func (p *Part) LowerBound() uint64 {
	if p.rng.Lower != nil && p.rng.Lower.RowIndex != nil {
		return uint64(*p.rng.Lower.RowIndex)
	}
	return 0
}

func (p *Part) UpperBound() uint64 {
	if p.rng.Upper != nil && p.rng.Upper.RowIndex != nil {
		return uint64(*p.rng.Upper.RowIndex)
	}
	return 0
}

func (p *Part) PartKey() PartKey {
	return &partKey{
		NodeID: p.nodeID,
		Table:  p.name,
		Rng:    p.rng,
	}
}

func (p *Part) TxID() yt.TxID {
	return p.txID
}

func (p *Part) NodeID() yt.NodeID {
	return p.nodeID
}

func (p *Part) ToTablePart() (*abstract.TableDescription, error) {
	lower := p.LowerBound()
	upper := p.UpperBound()
	key, err := p.PartKey().String()
	if err != nil {
		return nil, xerrors.Errorf("Can't make table part: %w", err)
	}
	return &abstract.TableDescription{
		Name:   p.Name(),
		Schema: "",
		Filter: abstract.WhereStatement(key),
		EtaRow: upper - lower,
		Offset: lower,
	}, nil
}

func NewPart(name string, nodeID yt.NodeID, rng ypath.Range, txID yt.TxID) *Part {
	return &Part{
		name:   name,
		nodeID: nodeID,
		rng:    rng,
		txID:   txID,
	}
}

func rangeToLegacyWhere(rng ypath.Range) abstract.WhereStatement {
	var res string
	if rng.Lower != nil && rng.Lower.RowIndex != nil {
		res += fmt.Sprintf("(%s >= %d)", RowIdxKey, *rng.Lower.RowIndex)
	}
	if rng.Upper != nil && rng.Upper.RowIndex != nil {
		cond := fmt.Sprintf("(%s < %d)", RowIdxKey, *rng.Upper.RowIndex)
		if len(res) != 0 {
			res += " AND " + cond
		} else {
			res = cond
		}
	}
	return abstract.WhereStatement(res)
}

var condPattern = "($row_index %s %d)"

// LegacyWhereToRange is now unused pair to rangeToLegacyWhere. May be needed later for incremental transfers, etc
func LegacyWhereToRange(where abstract.WhereStatement) (ypath.Range, error) {
	rng := ypath.Full()

	str := string(where)
	if str == "" {
		return rng, nil
	}
	conds := strings.Split(str, " AND ")

	if l := len(conds); l > 2 {
		return rng, xerrors.Errorf("too much where conditions (%d)", l)
	}
	for _, cond := range conds {
		var op string
		var val int64
		if _, err := fmt.Sscanf(cond, condPattern, &op, &val); err != nil {
			return rng, xerrors.Errorf("error parsing where condition %s: %w", cond, err)
		}
		switch op {
		case ">=":
			rng.Lower = &ypath.ReadLimit{RowIndex: &val}
		case "<":
			rng.Upper = &ypath.ReadLimit{RowIndex: &val}
		default:
			return rng, xerrors.Errorf("unknown operation %s", op)
		}
	}
	return rng, nil
}
