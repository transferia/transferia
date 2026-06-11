package queue_to_s3_parsequeue

import (
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsequeue"
	"go.ytsaurus.tech/library/go/core/log"
)

type QueueToS3WaitableParseQueue[TData any] struct {
	parseQueue *QueueToS3ParseQueue[TData]
	rawAck     QueueToS3AckFunc

	inflightProcesses atomic.Int32
}

func (p *QueueToS3WaitableParseQueue[TData]) Add(message TData) error {
	if err := p.parseQueue.Add(message); err != nil {
		return err
	}
	p.inflightProcesses.Add(1)
	return nil
}

// Wait waits when all messages, added via .Add() will be acked
//
// Should be called mutually exclusive with Add()/Close()
func (p *QueueToS3WaitableParseQueue[TData]) Wait() {
	for {
		select {
		case <-p.parseQueue.stopCh:
			return
		case <-time.After(50 * time.Millisecond):
			if p.inflightProcesses.Load() <= 0 {
				return
			}
		}
	}
}

func (p *QueueToS3WaitableParseQueue[TData]) ackWrapped(pushResult abstract.QueueResult) error {
	defer p.inflightProcesses.Add(-1)
	return p.rawAck(pushResult)
}

func (p *QueueToS3WaitableParseQueue[TData]) Close() {
	p.parseQueue.Close()
}

func (p *QueueToS3WaitableParseQueue[TData]) Error() error {
	return p.parseQueue.Error()
}

func (p *QueueToS3WaitableParseQueue[TData]) Done() <-chan struct{} {
	return p.parseQueue.Done()
}

func NewWaitable[TData any](
	lgr log.Logger,
	parallelism int,
	sink abstract.QueueToS3Sink,
	parseF parsequeue.ParseFunc[TData],
	ackF QueueToS3AckFunc,
) *QueueToS3WaitableParseQueue[TData] {
	parseQueue := &QueueToS3WaitableParseQueue[TData]{
		parseQueue: nil,
		rawAck:     ackF,

		inflightProcesses: atomic.Int32{},
	}
	parseQueue.parseQueue = New[TData](lgr, parallelism, sink, parseF, parseQueue.ackWrapped)
	return parseQueue
}
