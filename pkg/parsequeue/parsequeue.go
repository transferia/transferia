package parsequeue

import (
	"context"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type ParseFunc[TData any] func(TData) ([]abstract.ChangeItem, error)
type AckFunc[TData any] func(data TData, pushSt time.Time) error

type ParseQueue[TData any] struct {
	ctx    context.Context
	cancel func()

	wg sync.WaitGroup

	pushCh chan parseTask[TData]
	ackCh  chan pushTask[TData]

	errOnce  sync.Once
	firstErr error

	logger log.Logger

	sink   abstract.AsyncSink
	parseF ParseFunc[TData]
	ackF   AckFunc[TData]
}

type pushTask[T any] struct {
	errCh  chan error
	msg    T
	pushSt time.Time
}

type parseTask[T any] struct {
	msg   T
	resCh chan parseResult
}

type parseResult struct {
	data []abstract.ChangeItem
	err  error
}

var ErrorWhileParsing = xerrors.New("parse queue failed on sending parse task")

// Add will schedule new message parse
//
//	Do not call concurrently with Close()!
func (p *ParseQueue[TData]) Add(message TData) error {
	if !util.IsOpen(p.ctx.Done()) {
		return xerrors.New("parse queue is already closed")
	}
	if !util.Send(p.ctx, p.pushCh, p.makeParseTask(message)) {
		return xerrors.New("parse queue failed on sending parse task")
	}
	return nil
}

// Close shutdown all goroutines
//
//	Do not call concurrently with Add()
func (p *ParseQueue[TData]) Close() {
	p.cancel()
	p.wg.Wait()
}

// Error returns the first error that occurred during queue operation.
// Error state is guaranteed to be final after Close() completes.
//
// Safe to call concurrently and after Close().
func (p *ParseQueue[TData]) Error() error {
	if p.firstErr != nil {
		return xerrors.Errorf("parse queue: %w", p.firstErr)
	}
	return nil
}

func (p *ParseQueue[TData]) Done() <-chan struct{} {
	return p.ctx.Done()
}

func (p *ParseQueue[TData]) makeParseTask(items TData) parseTask[TData] {
	resCh := make(chan parseResult)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		parsedData, err := p.parseF(items)
		util.Send(p.ctx, resCh, parseResult{data: parsedData, err: err})
	}()
	return parseTask[TData]{msg: items, resCh: resCh}
}

func (p *ParseQueue[TData]) pushLoop() {
	defer p.wg.Done()
	for {
		parsed, ok := util.Receive(p.ctx, p.pushCh)
		if !ok {
			return
		}

		p.logger.Debug("wait for push")
		parseRes, ok := util.Receive(p.ctx, parsed.resCh)
		if !ok {
			return
		}

		if parseRes.err != nil {
			p.failWithError(xerrors.Errorf("parsing error: %w", parseRes.err))
			return
		}

		task := pushTask[TData]{
			errCh:  p.sink.AsyncPush(parseRes.data),
			msg:    parsed.msg,
			pushSt: time.Now(),
		}
		if !util.Send(p.ctx, p.ackCh, task) {
			return
		}
	}
}

func (p *ParseQueue[TData]) ackLoop() {
	defer p.wg.Done()
	for {
		ack, ok := util.Receive(p.ctx, p.ackCh)
		if !ok {
			return
		}

		p.logger.Debug("wait for ack")
		err, ok := util.Receive(p.ctx, ack.errCh)
		if !ok {
			return
		}
		if err != nil {
			p.failWithError(xerrors.Errorf("push error: %w", err))
			return
		}

		if err = p.ackF(ack.msg, ack.pushSt); err != nil {
			p.failWithError(xerrors.Errorf("ack error: %w", err))
			return
		}
	}
}

func (p *ParseQueue[TData]) failWithError(err error) {
	p.errOnce.Do(func() {
		p.firstErr = err
		p.logger.Warn("parse queue failed", log.Error(err))
		p.cancel()
	})
}

const DefaultParallelism = 10

func New[TData any](
	lgr log.Logger,
	parallelism int,
	sink abstract.AsyncSink,
	parseF ParseFunc[TData],
	ackF AckFunc[TData],
) *ParseQueue[TData] {
	if parallelism == 0 {
		parallelism = DefaultParallelism
	}
	if parallelism < 2 {
		parallelism = 2
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := &ParseQueue[TData]{
		wg: sync.WaitGroup{},

		ctx:    ctx,
		cancel: cancel,

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
		ackCh:  make(chan pushTask[TData], 1_000_000), // see: https://github.com/transferia/transferia/review/4480529/details#comment-6575167

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
