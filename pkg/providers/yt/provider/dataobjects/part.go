package dataobjects

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type Part struct {
	name     string
	nodeID   yt.NodeID
	ranges   []ypath.Range
	rowCount uint64
	txID     yt.TxID
	cookie   []byte
}

func (p *Part) Name() string {
	return p.name
}

func (p *Part) FullName() string {
	return p.name
}

func (p *Part) ToOldTableDescription() (*abstract.TableDescription, error) {
	tableDescription := &abstract.TableDescription{
		Name:   p.Name(),
		Schema: "",
		Filter: rangesToFilter(p.ranges),
		EtaRow: p.rowCount,
		Offset: p.LowerBound(),
	}
	tableDescription.SetPayload(append([]byte(nil), p.cookie...))
	return tableDescription, nil
}

func (p *Part) LowerBound() uint64 {
	if len(p.ranges) == 0 {
		return 0
	}
	first := p.ranges[0]
	if first.Lower != nil && first.Lower.RowIndex != nil {
		return uint64(*first.Lower.RowIndex)
	}
	return 0
}

func (p *Part) UpperBound() uint64 {
	if len(p.ranges) == 0 {
		return p.rowCount
	}
	last := p.ranges[len(p.ranges)-1]
	if last.Upper != nil && last.Upper.RowIndex != nil {
		return uint64(*last.Upper.RowIndex)
	}
	return p.LowerBound() + p.rowCount
}

func (p *Part) TxID() yt.TxID {
	return p.txID
}

func (p *Part) NodeID() yt.NodeID {
	return p.nodeID
}

func (p *Part) RowCount() uint64 {
	return p.rowCount
}

func (p *Part) Ranges() []ypath.Range {
	return append([]ypath.Range(nil), p.ranges...)
}

func (p *Part) Cookie() []byte {
	return append([]byte(nil), p.cookie...)
}

func (p *Part) ToTablePart() (*abstract.TableDescription, error) {
	tableDescription := &abstract.TableDescription{
		Name:   p.Name(),
		Schema: "",
		Filter: rangesToFilter(p.ranges),
		EtaRow: p.rowCount,
		Offset: p.LowerBound(),
	}
	tableDescription.SetPayload(append([]byte(nil), p.cookie...))
	return tableDescription, nil
}

func NewPart(name string, nodeID yt.NodeID, ranges []ypath.Range, rowCount uint64, cookie []byte, txID yt.TxID) *Part {
	return &Part{
		name:     name,
		nodeID:   nodeID,
		ranges:   append([]ypath.Range(nil), ranges...),
		rowCount: rowCount,
		txID:     txID,
		cookie:   append([]byte(nil), cookie...),
	}
}

func NewPartFromTableDescription(name string, nodeID yt.NodeID, table abstract.TableDescription, txID yt.TxID) (*Part, error) {
	ranges, err := FilterToRanges(table.Filter)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse table part ranges from filter %q: %w", table.Filter, err)
	}
	return NewPart(name, nodeID, ranges, table.EtaRow, table.GetPayload(), txID), nil
}

func NewPartFromPartition(name string, nodeID yt.NodeID, partition yt.TablePartition, txID yt.TxID) (*Part, error) {
	if len(partition.Cookie) == 0 {
		return nil, xerrors.New("partition cookie is empty")
	}
	ranges, err := TablePartitionRanges(partition)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract partition ranges: %w", err)
	}
	rowCount, err := partitionRowCount(partition)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract partition row count: %w", err)
	}
	return NewPart(name, nodeID, ranges, rowCount, partition.Cookie, txID), nil
}

func TablePartitionRanges(partition yt.TablePartition) ([]ypath.Range, error) {
	res := make([]ypath.Range, 0)
	for _, tableRange := range partition.TableRanges {
		for _, rng := range tableRange.Ranges {
			if rng.Exact != nil {
				return nil, xerrors.New("exact table partition ranges are not supported")
			}
			res = append(res, rng)
		}
	}
	if len(res) == 0 {
		return nil, xerrors.New("table partition has no row ranges")
	}
	return res, nil
}

func partitionRowCount(partition yt.TablePartition) (uint64, error) {
	if partition.AggregateStatistics.RowCount < 0 {
		return 0, xerrors.Errorf("partition row count is negative: %d", partition.AggregateStatistics.RowCount)
	}
	return uint64(partition.AggregateStatistics.RowCount), nil
}

func rangesToFilter(ranges []ypath.Range) abstract.WhereStatement {
	if len(ranges) == 0 {
		return ""
	}

	items := make([]string, 0, len(ranges))
	for _, rng := range ranges {
		lower := ""
		if rng.Lower != nil && rng.Lower.RowIndex != nil {
			lower = strconv.FormatInt(*rng.Lower.RowIndex, 10)
		}
		upper := ""
		if rng.Upper != nil && rng.Upper.RowIndex != nil {
			upper = strconv.FormatInt(*rng.Upper.RowIndex, 10)
		}
		items = append(items, fmt.Sprintf("%s:%s", lower, upper))
	}
	return abstract.WhereStatement("rows=[" + strings.Join(items, ",") + "]")
}

func FilterToRanges(filter abstract.WhereStatement) ([]ypath.Range, error) {
	const prefix = "rows=["
	const suffix = "]"

	str := string(filter)
	if str == "" {
		return nil, xerrors.New("empty partition filter")
	}
	if !strings.HasPrefix(str, prefix) || !strings.HasSuffix(str, suffix) {
		return nil, xerrors.Errorf("unexpected partition filter format: %s", str)
	}

	body := strings.TrimSuffix(strings.TrimPrefix(str, prefix), suffix)
	if body == "" {
		return nil, xerrors.New("partition filter contains no ranges")
	}
	items := strings.Split(body, ",")
	res := make([]ypath.Range, 0, len(items))
	for _, item := range items {
		bounds := strings.Split(item, ":")
		if len(bounds) != 2 {
			return nil, xerrors.Errorf("invalid partition range %q", item)
		}
		var rng ypath.Range
		if bounds[0] != "" {
			lower, err := strconv.ParseInt(bounds[0], 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("invalid lower range bound %q: %w", bounds[0], err)
			}
			rng.Lower = &ypath.ReadLimit{RowIndex: &lower}
		}
		if bounds[1] != "" {
			upper, err := strconv.ParseInt(bounds[1], 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("invalid upper range bound %q: %w", bounds[1], err)
			}
			rng.Upper = &ypath.ReadLimit{RowIndex: &upper}
		}
		res = append(res, rng)
	}
	return res, nil
}
