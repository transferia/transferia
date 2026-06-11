package parsequeue

import (
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type WaitableParseQueue[TData any] struct {
	parseQueue *ParseQueue[TData]
	rawAck     AckFunc[TData]

	inflightProcesses atomic.Int32
}

func (p *WaitableParseQueue[TData]) Add(message TData) error {
	if err := p.parseQueue.Add(message); err != nil {
		return err
	}
	p.inflightProcesses.Add(1)
	return nil
}

// Wait waits when all messages, added via .Add() will be acked
//
// Should be called mutually exclusive with Add()/Close()
func (p *WaitableParseQueue[TData]) Wait() {
	for {
		select {
		case <-p.parseQueue.ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
			if p.inflightProcesses.Load() == 0 {
				return
			}
		}
	}
}

func (p *WaitableParseQueue[TData]) ackWrapped(data TData, pushSt time.Time) error {
	defer p.inflightProcesses.Add(-1)
	return p.rawAck(data, pushSt)
}

func (p *WaitableParseQueue[TData]) Close() {
	p.parseQueue.Close()
}

func (p *WaitableParseQueue[TData]) Error() error {
	return p.parseQueue.Error()
}

func (p *WaitableParseQueue[TData]) Done() <-chan struct{} {
	return p.parseQueue.Done()
}

func NewWaitable[TData any](
	lgr log.Logger,
	parallelism int,
	sink abstract.AsyncSink,
	parseF ParseFunc[TData],
	ackF AckFunc[TData],
) *WaitableParseQueue[TData] {
	parseQueue := &WaitableParseQueue[TData]{
		parseQueue: nil,
		rawAck:     ackF,

		inflightProcesses: atomic.Int32{},
	}
	parseQueue.parseQueue = New[TData](lgr, parallelism, sink, parseF, parseQueue.ackWrapped)
	return parseQueue
}
