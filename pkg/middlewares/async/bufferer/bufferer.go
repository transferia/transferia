package bufferer

import (
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/stats"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

// Bufferer buffers AsyncPush operations by storing items in a built-in buffer. The buffer is flushed when any of the following conditions is fulfilled:
//   - The number of buffered items is greater or equal to config.TriggingCount; OR
//   - The Value-size of buffered items is greater or equal to config.TriggingSize; OR
//   - config.TriggingInterval has passed since the start of the last flush; OR
//   - A non-row item is detected among the incoming items; OR
//   - The middleware is closed.
//
// Any config property can be set to its default (zero) value, in which case it does not apply.
func Bufferer(logger log.Logger, config BuffererConfig, r metrics.Registry) func(abstract.Sinker) abstract.AsyncSink {
	return func(s abstract.Sinker) abstract.AsyncSink {
		return newBufferer(s, logger, r, config.TriggingCount, config.TriggingSize, config.TriggingInterval)
	}
}

type BuffererConfig struct {
	TriggingCount    int           // items count (of all of the items buffered) triggering a flush
	TriggingSize     uint64        // byte size / Values size (of all of the items buffered) triggering a flush
	TriggingInterval time.Duration // interval triggering a flush, measured from the start of the last flush - which might be caused by other triggers
}

type bufferer struct {
	apiState *apiState

	sink abstract.Sinker

	TriggingCount    int
	TriggingSize     uint64
	TriggingInterval time.Duration

	sta                 *stats.MiddlewareBuffererStats
	lastCollectionStart time.Time

	logger log.Logger
}

func newBufferer(sink abstract.Sinker, logger log.Logger, r metrics.Registry, triggingCount int, triggingSize uint64, triggingInterval time.Duration) *bufferer {
	result := &bufferer{
		apiState: newAPIState(),

		sink: sink,

		TriggingCount:    triggingCount,
		TriggingSize:     triggingSize,
		TriggingInterval: triggingInterval,

		sta:                 stats.NewMiddlewareBuffererStats(r),
		lastCollectionStart: time.Now(),

		logger: logger,
	}

	go result.run()

	return result
}

// apiState contains the state connecting an asynchronously running goroutine which actually performs Push and callers of the middleware
type apiState struct {
	sync.RWMutex
	sync.WaitGroup

	Closed bool

	InputCh chan inputItem
}

func newAPIState() *apiState {
	result := apiState{
		RWMutex:   sync.RWMutex{},
		WaitGroup: sync.WaitGroup{},

		Closed: false,

		InputCh: make(chan inputItem),
	}
	result.Add(1)
	return &result
}

type inputItem struct {
	Items []abstract.ChangeItem
	ErrCh chan error
}

// Close blocks until all buffered items are pushed.
// The error returned is the error of underlying sink's Close, not an error of some buffered Push.
func (b *bufferer) Close() error {
	b.apiState.Lock()
	defer b.apiState.Unlock()

	if b.apiState.Closed {
		return nil
	}
	b.apiState.Closed = true

	close(b.apiState.InputCh)
	b.apiState.Wait()

	return b.sink.Close()
}

// AsyncPush blocks if an append to the buffer is not possible. This provides backpressure to the source.
func (b *bufferer) AsyncPush(items []abstract.ChangeItem) chan error {
	b.apiState.RLock()
	defer b.apiState.RUnlock()

	result := make(chan error, 1)

	if b.apiState.Closed {
		result <- abstract.AsyncPushConcurrencyErr
		return result
	}

	if len(items) == 0 {
		result <- nil
		return result
	}

	b.apiState.InputCh <- inputItem{
		Items: items,
		ErrCh: result,
	}

	return result
}

// bufferFlushState inherits sync.WaitGroup and contains all the information to do and control flush
type buffererFlushState struct {
	sync.WaitGroup

	// Buffer to flush at the next flush
	Buffer *buffer

	// Num is the number of current flush
	Num int
	// Timer to wait for flush
	Timer *util.SmartTimer
}

func newBuffererFlushState(triggingInterval time.Duration) *buffererFlushState {
	return &buffererFlushState{
		WaitGroup: sync.WaitGroup{},

		Buffer: newBuffer(),

		Num:   0,
		Timer: util.NewSmartTimer(triggingInterval),
	}
}

func (b *bufferer) run() {
	defer b.apiState.Done()

	flushState := newBuffererFlushState(b.TriggingInterval)

	// The first push must happen AFTER the interval passes
	flushState.Timer.Restart()

	for {
		select {
		case inputItem, ok := <-b.apiState.InputCh:
			if !ok {
				b.logger.Info("Flush is triggered by Close", log.Int("len", flushState.Buffer.Len()), log.String("size", humanize.Bytes(flushState.Buffer.ValuesSize())), log.Int("flush_num", flushState.Num))
				b.sta.FlushOnNonRow.Inc()
				b.flush(flushState)
				flushState.Wait()
				return
			}

			flushState.Buffer.Add(inputItem.Items, inputItem.ErrCh)

			if b.TriggingCount > 0 && flushState.Buffer.Len() >= b.TriggingCount {
				b.logger.Info("Flush is triggered by items count", log.Int("len", flushState.Buffer.Len()), log.String("size", humanize.Bytes(flushState.Buffer.ValuesSize())), log.Int("trigging_len", b.TriggingCount), log.Int("flush_num", flushState.Num))
				b.sta.FlushOnCount.Inc()
				b.flush(flushState)
				continue
			}
			if b.TriggingSize > 0 && flushState.Buffer.ValuesSize() >= b.TriggingSize {
				b.logger.Info("Flush is triggered by items size", log.Int("len", flushState.Buffer.Len()), log.String("size", humanize.Bytes(flushState.Buffer.ValuesSize())), log.String("trigging_size", humanize.Bytes(b.TriggingSize)), log.Int("flush_num", flushState.Num))
				b.sta.FlushOnSize.Inc()
				b.flush(flushState)
				continue
			}
			if b.TriggingInterval > 0 && flushState.Timer.HasFired() {
				b.logger.Info("Flush is triggered by exceeded interval", log.Int("len", flushState.Buffer.Len()), log.String("size", humanize.Bytes(flushState.Buffer.ValuesSize())), log.Duration("trigging_interval", b.TriggingInterval), log.Int("flush_num", flushState.Num))
				b.sta.FlushOnInterval.Inc()
				b.flush(flushState)
				continue
			}
			if abstract.ContainsNonRowItem(inputItem.Items) {
				b.logger.Info("Flush is triggered by a non-row item", log.Int("len", flushState.Buffer.Len()), log.String("size", humanize.Bytes(flushState.Buffer.ValuesSize())), log.Int("flush_num", flushState.Num))
				b.sta.FlushOnNonRow.Inc()
				b.flush(flushState)
				continue
			}
		case <-flushState.Timer.C():
			if flushState.Buffer.Len() > 0 {
				b.logger.Info("Flush is triggered by interval", log.Int("len", flushState.Buffer.Len()), log.String("size", humanize.Bytes(flushState.Buffer.ValuesSize())), log.Duration("trigging_interval", b.TriggingInterval), log.Int("flush_num", flushState.Num))
				b.sta.FlushOnInterval.Inc()
				b.flush(flushState)
				continue
			}
		}
	}
}

func (b *bufferer) flush(flushState *buffererFlushState) {
	b.sta.CollectionTime.RecordDuration(time.Since(b.lastCollectionStart))
	b.sta.FlushOnAllCauses.Inc()

	waitStart := time.Now()
	// There may be another flush in progress. Wait until it finishes and do not return from flush.
	// This (in combination with the implementation of the goroutine that calls "flush") provides backpressure to the source.
	flushState.Wait()
	b.sta.WaitTime.RecordDuration(time.Since(waitStart))

	toFlush := flushState.Buffer
	flushState.Buffer = newBuffer()

	b.sta.CountToFlush.RecordValue(float64(toFlush.Len()))
	b.sta.SizeToFlush.RecordValue(float64(toFlush.ValuesSize()))

	flushState.Add(1)
	go func(num int) {
		toFlush.Flush(b.sink.Push)
		b.logger.Info("Flush has finished", log.Int("flush_num", num))
		flushState.Done()
	}(flushState.Num)

	flushState.Num += 1

	// immediately restart the timer. Its next tick may cause flush before the previous one finishes, and this is fine
	flushState.Timer.Restart()
	b.lastCollectionStart = time.Now()
}
