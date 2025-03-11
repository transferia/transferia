package events

import (
	"encoding/binary"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/base/types"
	"go.ytsaurus.tech/yt/go/yson"
)

type ColumnValues interface {
	Value(i int) base.Value
	Column() base.Column
}

type baseValues struct {
	values []base.Value
	col    base.Column
}

func (b baseValues) Value(i int) base.Value {
	return b.values[i]
}

func (b baseValues) Column() base.Column {
	return b.col
}

type TableEventsBuilder interface {
	AddRow() RowBuilder
	AsBatch() base.EventBatch
}

type tableEventsBuilder struct {
	table  base.Table
	cols   []*baseValues
	colIDX map[string]int
	iter   int
}

type insertsBatch struct {
	table  base.Table
	cols   []*baseValues
	colIDX map[string]int
	iter   int
}

func (b *insertsBatch) MarshalYSON(w *yson.Writer) error {
	w.BeginList()
	for b.Next() {
		w.BeginMap()
		for i := 0; i < b.table.ColumnsCount(); i++ {
			val := b.cols[i].values[b.iter]
			w.MapKeyString(val.Column().FullName())
			if val.Value() == nil {
				w.Entity()
				continue
			}
			switch v := val.(type) {
			case types.StringValue:
				w.String(*v.StringValue())
			case types.Int8Value:
				w.Int64(int64(*v.Int8Value()))
			case types.Int16Value:
				w.Int64(int64(*v.Int16Value()))
			case types.Int32Value:
				w.Int64(int64(*v.Int32Value()))
			case types.Int64Value:
				w.Int64(*v.Int64Value())
			case types.UInt8Value:
				w.Uint64(uint64(*v.UInt8Value()))
			case types.UInt16Value:
				w.Uint64(uint64(*v.UInt16Value()))
			case types.UInt32Value:
				w.Uint64(uint64(*v.UInt32Value()))
			case types.UInt64Value:
				w.Uint64(uint64(*v.UInt64Value()))
			case types.FloatValue:
				w.Float64(float64(*v.FloatValue()))
			case types.DoubleValue:
				w.Float64(*v.DoubleValue())
			case types.DecimalValue:
				w.String(*v.DecimalValue())
			case types.BigFloatValue:
				w.String(*v.BigFloatValue())
			case types.TimestampValue:
				w.String(v.TimestampValue().String())
			case types.TimestampTZValue:
				w.String(v.TimestampTZValue().String())
			case types.DateValue:
				w.String(v.DateValue().String())
			case types.DateTimeValue:
				w.String(v.DateTimeValue().String())
			case types.IntervalValue:
				w.Int64(v.IntervalValue().Nanoseconds())
			default:
				return xerrors.Errorf("unknown value format: %T", val)
			}
		}
		w.EndMap()
	}
	w.EndList()
	return nil
}

func (b *insertsBatch) Next() bool {
	if len(b.cols[0].values)-1 > b.iter {
		b.iter++
		return true
	}
	return false
}

func (b *insertsBatch) Count() int {
	return len(b.cols[0].values)
}

func (b *insertsBatch) Size() int {
	return binary.Size(b.cols)
}

type insertBatchEvent struct {
	batch *insertsBatch
	iter  int
}

func (e insertBatchEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	ev := NewDefaultInsertEvent(e.batch.table)
	for i := 0; i < e.batch.table.ColumnsCount(); i++ {
		colIDX := e.batch.colIDX[e.batch.table.Column(i).FullName()]
		col := e.batch.cols[colIDX]
		if err := ev.AddNewValue(col.values[e.iter]); err != nil {
			return nil, xerrors.Errorf("unable to generate insert event: %w", err)
		}
	}
	return ev.ToOldChangeItem()
}

func (e insertBatchEvent) Table() base.Table {
	return e.batch.table
}

func (e insertBatchEvent) NewValuesCount() int {
	return e.batch.table.ColumnsCount()
}

func (e insertBatchEvent) NewValue(i int) (base.Value, error) {
	return e.batch.cols[e.iter].values[i], nil
}

func (b *insertsBatch) Event() (base.Event, error) {
	return &insertBatchEvent{
		batch: b,
		iter:  b.iter,
	}, nil
}

func (t *tableEventsBuilder) AsBatch() base.EventBatch {
	return &insertsBatch{
		table:  t.table,
		cols:   t.cols,
		colIDX: t.colIDX,
		iter:   -1,
	}
}

func (t *tableEventsBuilder) AddRow() RowBuilder {
	for _, col := range t.cols {
		col.values = append(col.values, nil)
	}
	t.iter++
	return &rowBuilder{
		tableBuilder: t,
		rowIDX:       t.iter,
	}
}

type RowBuilder interface {
	AddValue(val base.Value)
}

type rowBuilder struct {
	tableBuilder *tableEventsBuilder
	rowIDX       int
}

func (r rowBuilder) AddValue(val base.Value) {
	r.tableBuilder.cols[r.tableBuilder.colIDX[val.Column().FullName()]].values[r.rowIDX] = val
}

func NewTableEventsBuilder(table base.Table) TableEventsBuilder {
	colIDX := map[string]int{}
	var colValues []*baseValues
	for i := 0; i < table.ColumnsCount(); i++ {
		col := table.Column(i)
		colIDX[col.FullName()] = i
		colValues = append(colValues, &baseValues{
			values: make([]base.Value, 0),
			col:    col,
		})
	}
	return &tableEventsBuilder{
		table:  table,
		cols:   colValues,
		colIDX: colIDX,
		iter:   -1,
	}
}
