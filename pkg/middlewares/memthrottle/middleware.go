package memthrottle

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/constraints"
)

// defaultMinReserve should cover Go runtime resources + all background tasks
// like sending logs, metrics, upload state, etc.
// It may also help to survive spontaneous large batch from source
const defaultMinReserve = 200 * humanize.MiByte

type Config struct {
	totalBytes          uint64
	maxAvailableBytes   uint64
	startAvailableBytes uint64
}

// DefaultConfig makes default config for memory throttler algo
//
// totalBytes parameter should define total memory amount available
// to the process, not VM size or smth
func DefaultConfig(totalBytes uint64) Config {
	return Config{
		totalBytes: totalBytes,
		// Must have some reserve for some random spikes/algo inaccuracies/background processes/etc
		maxAvailableBytes: totalBytes - defaultMinReserve,
		// Initial assumption is that equal memory is needed on both source and sink pipeline sides
		startAvailableBytes: (totalBytes - defaultMinReserve) / 2,
	}
}

// MemoryThrottler is middleware that triggers buffer flush (and GC)
// if memory consumption is close to limit
//
// MemoryThrottler is coupled with async.Bufferer and
// relies on immediate buffer flush if non-row item has been encountered. It doesn't
// work without Bufferer and should not work well with sources pushing big chunks of data
// in single AsyncPush call. It's believed that all sources should produce reasonably small batches in future
//
// It use simple adaptive algorithm to determine and control flush threshold
// depending on current memory usage
func MemoryThrottler(cfg Config, lgr log.Logger) abstract.AsyncMiddleware {
	return func(sink abstract.AsyncSink) abstract.AsyncSink {
		return newMemoryThrottler(cfg, lgr, sink)
	}
}

type memoryThrottler struct {
	sink               abstract.AsyncSink
	lgr                log.Logger
	mtx                sync.Mutex
	currAvailableBytes int64
	maxAvailableBytes  int64
}

func newMemoryThrottler(cfg Config, lgr log.Logger, sink abstract.AsyncSink) abstract.AsyncSink {
	return &memoryThrottler{
		sink:               sink,
		lgr:                lgr,
		maxAvailableBytes:  int64(cfg.maxAvailableBytes),
		currAvailableBytes: int64(cfg.startAvailableBytes),
		mtx:                sync.Mutex{},
	}
}

func (m *memoryThrottler) AsyncPush(items []abstract.ChangeItem) chan error {
	if err := m.flushIfNeeded(); err != nil {
		return util.MakeChanWithError(xerrors.Errorf("error while triggering buffer flush: %w", err))
	}

	return m.sink.AsyncPush(items)
}

func (m *memoryThrottler) Close() error {
	return m.sink.Close()
}

func (m *memoryThrottler) shouldFlush(used int64) bool {
	if used >= m.currAvailableBytes {
		m.lgr.Info(fmt.Sprintf("Used %s memory, limit is %s, should flush", formatMBytes(used), formatMBytes(m.currAvailableBytes)),
			log.Int64("usage", used), log.Int64("limit", m.currAvailableBytes))
		return true
	}
	return false
}

func (m *memoryThrottler) flushIfNeeded() error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	used := m.usedBytes()
	if m.shouldFlush(used) {
		err := <-m.sink.AsyncPush([]abstract.ChangeItem{abstract.MakeSynchronizeEvent()})
		m.tryIncreaseThreshold(used)
		debug.FreeOSMemory() // Force GC just like GOMEMLIMIT does
		return err
	}
	return nil
}

func (m *memoryThrottler) tryIncreaseThreshold(usedBefore int64) {
	usedAfter := m.usedBytes()
	pushDelta := usedAfter - usedBefore

	// memory usage is under current threshold or GC has occured or smth,
	// just have enough memory and continue with current threshold
	if pushDelta < 0 && usedAfter < m.currAvailableBytes {
		return
	}

	availDelta := m.maxAvailableBytes - usedAfter
	if availDelta < 0 {
		// if mem usage is over limit - reduce threshold significantly to fit
		m.currAvailableBytes += 2 * availDelta
	} else if availDelta > pushDelta*2 {
		// increase threshold only if there's enough mem to survive two simultaneous flushes
		// 4 - to split available mem for 2 pushes * (half for source + half for sink pipeline)
		m.currAvailableBytes += availDelta / 4
	}
	m.lgr.Info(fmt.Sprintf("Updated available bytes, new value is %s, used memory after push is %s", formatMBytes(m.currAvailableBytes), formatMBytes(usedAfter)),
		log.Int64("limit", m.currAvailableBytes), log.Int64("usage", usedAfter))
}

func (m *memoryThrottler) usedBytes() int64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	m.lgr.Debugf("Mem stats: HeapAlloc=%s, HeapInuse=%s, HeapIdle=%s, HeapSys=%s, HeapReleased=%s, Sys=%s",
		formatMBytes(stats.HeapAlloc),
		formatMBytes(stats.HeapInuse),
		formatMBytes(stats.HeapIdle),
		formatMBytes(stats.HeapSys),
		formatMBytes(stats.HeapReleased),
		formatMBytes(stats.Sys))

	// Total virtual mem minus released to OS. See https://tip.golang.org/doc/gc-guide for details
	return int64(stats.Sys - stats.HeapReleased)
}

func formatMBytes[T constraints.Integer](bytes T) string {
	return fmt.Sprintf("%0.3f", float64(bytes)/humanize.MiByte)
}
