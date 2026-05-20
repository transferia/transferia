package queue_to_s3_parsequeue

import (
	"context"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsequeue"
	"go.ytsaurus.tech/library/go/core/log"
)

type QueueToS3AckFunc func(pushResult abstract.QueueResult) error

type QueueToS3ParseQueue[TData any] struct {
	wg       sync.WaitGroup
	stopCh   chan struct{}
	stopOnce sync.Once

	pushCh chan parseTask[TData]
	ackCh  chan abstract.AsyncPushResult

	errOnce  sync.Once
	firstErr error

	logger log.Logger

	sink   abstract.QueueToS3Sink
	parseF parsequeue.ParseFunc[TData]
	ackF   QueueToS3AckFunc
}

type parseTask[T any] struct {
	msg   T
	resCh chan parseRes
}

type parseRes struct {
	data []abstract.ChangeItem
	err  error
}

// Add will schedule new message parse
// Do not call concurrently with Close()!
func (p *QueueToS3ParseQueue[TData]) Add(message TData) error {
	select {
	case <-p.stopCh:
		return xerrors.New("parse queue is already closed")
	default:
	}

	select {
	case <-p.stopCh:
		return parsequeue.ErrorWhileParsing
	case p.pushCh <- p.makeParseTask(message):
		return nil
	}
}

// Close shutdown all goroutines
// Do not call concurrently with Add()
func (p *QueueToS3ParseQueue[TData]) Close() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	p.wg.Wait()
}

// Error returns the first error that occurred during queue operation.
// Error state is guaranteed to be final after Close() completes.
// Safe to call concurrently and after Close().
func (p *QueueToS3ParseQueue[TData]) Error() error {
	return p.firstErr
}

func (p *QueueToS3ParseQueue[TData]) makeParseTask(items TData) parseTask[TData] {
	resCh := make(chan parseRes)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		parseResult, err := p.parseF(items)
		select {
		case <-p.stopCh:
		case resCh <- parseRes{data: parseResult, err: err}:
		}
	}()
	return parseTask[TData]{msg: items, resCh: resCh}
}

func (p *QueueToS3ParseQueue[TData]) pushLoop() {
	defer p.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		var parsed parseTask[TData]
		select {
		case <-p.stopCh:
			return
		case parsed = <-p.pushCh:
		}

		p.logger.Debug("wait for push")
		var parseRes parseRes
		select {
		case <-p.stopCh:
			return
		case parseRes = <-parsed.resCh:
		}

		if parseRes.err != nil {
			p.failWithError(xerrors.Errorf("parse error: %w", parseRes.err))
		} else {
			p.sink.AsyncV2Push(ctx, p.ackCh, parseRes.data)
		}
	}
}

func (p *QueueToS3ParseQueue[TData]) ackLoop() {
	defer p.wg.Done()
	for {
		var pushResultForAck abstract.AsyncPushResult
		select {
		case <-p.stopCh:
			return
		case pushResultForAck = <-p.ackCh:
		}

		if pushResultForAck.GetError() != nil {
			p.failWithError(xerrors.Errorf("push error: %w", pushResultForAck.GetError()))
			return
		}

		queueResult, ok := pushResultForAck.GetResult().(abstract.QueueResult)
		if !ok {
			p.failWithError(xerrors.Errorf("unexpected push result type: %v", pushResultForAck.GetResult()))
			return
		}

		if err := p.ackF(queueResult); err != nil {
			p.failWithError(xerrors.Errorf("ack error: %w", err))
			return
		}
	}
}

func (p *QueueToS3ParseQueue[TData]) failWithError(err error) {
	if err == nil {
		return
	}

	p.errOnce.Do(func() {
		p.firstErr = err
		p.logger.Warn("parse queue to s3 failed", log.Error(err))
		p.stopOnce.Do(func() {
			close(p.stopCh)
		})
	})
}

func New[TData any](
	lgr log.Logger,
	parallelism int,
	sink abstract.QueueToS3Sink,
	parseF parsequeue.ParseFunc[TData],
	ackF QueueToS3AckFunc,
) *QueueToS3ParseQueue[TData] {
	if parallelism == 0 {
		parallelism = parsequeue.DefaultParallelism
	}
	if parallelism < 2 {
		parallelism = 2
	}

	result := &QueueToS3ParseQueue[TData]{
		wg:       sync.WaitGroup{},
		stopCh:   make(chan struct{}),
		stopOnce: sync.Once{},

		// I'm too lazy to explain why -2, but oleg - not:
		// Let's say you've set the parallelism to 10. The first time you call Add(),
		// a parsing goroutine is created, and a task to read the parsing result is placed into a channel.
		// Since pushLoop is not busy, it immediately reads this task from the channel
		// and waits during the read operation from resCh. At this point, there are 8 spots left in the pushCh
		// channel (10 - 2 == 8).
		//
		// Afterwards, you call Add() 8 more times, and your pushCh channel becomes filled with tasks.
		// So far, you've made 9 calls in total.
		//
		// Then, you call Add() again. The makeParseTask function is invoked, and a parsing goroutine starts.
		// However, we get blocked while writing to the pushCh channel, as there are already 8 buffered messages.
		// In total, this adds up to 8 + 2 = 10, which matches the code logic.
		//
		// This is a bit of a messy situation, indeed.
		pushCh: make(chan parseTask[TData], parallelism-2),
		ackCh:  make(chan abstract.AsyncPushResult, 1_000_000), // see: https://github.com/transferia/transferia/review/4480529/details#comment-6575167

		errOnce:  sync.Once{},
		firstErr: nil,

		logger: lgr,

		sink:   sink,
		parseF: parseF,
		ackF:   ackF,
	}
	result.wg.Add(2)
	go result.pushLoop()
	go result.ackLoop()
	return result
}
