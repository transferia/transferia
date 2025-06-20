//go:build !disable_yt_provider

package events

import (
	"encoding/binary"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers/yt/tablemeta"
)

type EventBatch struct {
	pos     int
	doneCnt uint64
	tables  tablemeta.YtTables
}

func (e *EventBatch) Next() bool {
	if e.pos < (len(e.tables) - 1) {
		e.pos += 1
		return true
	}
	return false
}

func (e *EventBatch) Count() int {
	return len(e.tables)
}

func (e *EventBatch) Size() int {
	return binary.Size(e.tables)
}

func (e *EventBatch) Event() (base.Event, error) {
	if e.pos >= len(e.tables) {
		return nil, xerrors.New("no more events in batch")
	}
	if e.pos < 0 {
		return nil, xerrors.New("invalid batch state, pos is < 0, maybe need to call Next first")
	}

	return newTableEvent(e.tables[e.pos]), nil
}

func (e *EventBatch) Progress() base.EventSourceProgress {
	total := uint64(len(e.tables))
	return base.NewDefaultEventSourceProgress(e.doneCnt == total, e.doneCnt, total)
}

func (e *EventBatch) TableProcessed() {
	e.doneCnt++
}

func NewEventBatch(tables tablemeta.YtTables) *EventBatch {
	return &EventBatch{
		pos:     -1,
		tables:  tables,
		doneCnt: 0,
	}
}
