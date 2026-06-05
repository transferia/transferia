package events

import (
	"encoding/binary"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
)

type EventBatch struct {
	pos     int
	doneCnt uint64
	nodes   cypressmeta.YtNodes
}

func (e *EventBatch) Next() bool {
	if e.pos < (len(e.nodes) - 1) {
		e.pos += 1
		return true
	}
	return false
}

func (e *EventBatch) Count() int {
	return len(e.nodes)
}

func (e *EventBatch) Size() int {
	return binary.Size(e.nodes)
}

func (e *EventBatch) Event() (abstract2.Event, error) {
	if e.pos >= len(e.nodes) {
		return nil, xerrors.New("no more events in batch")
	}
	if e.pos < 0 {
		return nil, xerrors.New("invalid batch state, pos is < 0, maybe need to call Next first")
	}

	return newNodeEvent(e.nodes[e.pos]), nil
}

func (e *EventBatch) Progress() abstract2.EventSourceProgress {
	total := uint64(len(e.nodes))
	return abstract2.NewDefaultEventSourceProgress(e.doneCnt == total, e.doneCnt, total)
}

func (e *EventBatch) TableProcessed() {
	e.doneCnt++
}

func NewEventBatch(nodes cypressmeta.YtNodes) *EventBatch {
	return &EventBatch{
		pos:     -1,
		nodes:   nodes,
		doneCnt: 0,
	}
}
