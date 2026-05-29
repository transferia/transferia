package provider

import (
	"github.com/transferia/transferia/pkg/abstract2"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
)

type decodedRow struct {
	values  []interface{}
	rowIDX  int64
	rawSize int
}

func (d *decodedRow) RawSize() int {
	return d.rawSize
}

type batch struct {
	rows   []decodedRow
	idx    int
	table  yt_table.YtTable
	part   string
	idxCol string
}

func (b *batch) Next() bool {
	b.idx++
	return len(b.rows) > b.idx
}

func (b *batch) Count() int {
	return len(b.rows)
}

func (b *batch) Size() int {
	var size int
	for _, row := range b.rows {
		size += row.rawSize
	}
	return size
}

func (b *batch) Event() (abstract2.Event, error) {
	return NewEventFromDecodedRow(b, b.idx), nil
}

func (b *batch) Append(row decodedRow) {
	b.rows = append(b.rows, row)
}

func (b *batch) Len() int {
	return len(b.rows)
}

func newEmptyBatch(tbl yt_table.YtTable, size int, part, idxCol string) *batch {
	return &batch{
		rows:   make([]decodedRow, 0, size),
		idx:    -1,
		table:  tbl,
		part:   part,
		idxCol: idxCol,
	}
}
