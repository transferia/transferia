//go:build !disable_yt_provider

package provider

import (
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers/yt/provider/table"
)

type lazyYSON struct {
	data   []byte
	rowIDX int64
}

func (l *lazyYSON) UnmarshalYSON(input []byte) error {
	l.data = make([]byte, len(input))
	copy(l.data, input)
	return nil
}

func (l *lazyYSON) RawSize() int {
	return len(l.data)
}

type batch struct {
	rows   []lazyYSON
	idx    int
	table  table.YtTable
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
		size += row.RawSize()
	}
	return size
}

func (b *batch) Event() (base.Event, error) {
	return NewEventFromLazyYSON(b, b.idx), nil
}

func (b *batch) Append(row lazyYSON) {
	b.rows = append(b.rows, row)
}

func (b *batch) Len() int {
	return len(b.rows)
}

func newEmptyBatch(tbl table.YtTable, size int, part, idxCol string) *batch {
	return &batch{
		rows:   make([]lazyYSON, 0, size),
		idx:    -1,
		table:  tbl,
		part:   part,
		idxCol: idxCol,
	}
}
