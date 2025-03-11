package parsequeue

import (
	"sync"
	"time"

	"github.com/transferria/transferria/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type WaitableParseQueue[TData any] struct {
	parseQueue *ParseQueue[TData]
	inflightWG sync.WaitGroup
	rawAck     AckFunc[TData]
}

func (p *WaitableParseQueue[TData]) Add(message TData) error {
	p.inflightWG.Add(1)
	return p.parseQueue.Add(message)
}

// Wait waits when all messages, added via .Add() will be acked
//
// Should be called mutually exclusive with Add()/Close()
func (p *WaitableParseQueue[TData]) Wait() {
	p.inflightWG.Wait()
}

func (p *WaitableParseQueue[TData]) ackWrapped(data TData, pushSt time.Time, err error) {
	p.rawAck(data, pushSt, err)
	p.inflightWG.Done()
}

func (p *WaitableParseQueue[TData]) Close() {
	p.parseQueue.Close()
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
		inflightWG: sync.WaitGroup{},
		rawAck:     ackF,
	}
	parseQueue.parseQueue = New[TData](lgr, parallelism, sink, parseF, parseQueue.ackWrapped)
	return parseQueue
}
