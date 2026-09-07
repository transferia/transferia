package throttler

import (
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/pkg/format"
	"go.ytsaurus.tech/library/go/core/log"
)

type MemoryThrottler struct {
	BufferSize    uint64 // 0 means turned-off
	inflightMutex sync.RWMutex
	inflightBytes uint64
}

func (t *MemoryThrottler) ExceededLimits() bool {
	t.inflightMutex.RLock()
	defer t.inflightMutex.RUnlock()
	return t.inflightBytes >= t.BufferSize && t.BufferSize != 0
}

func (t *MemoryThrottler) AddInflight(size uint64) {
	t.inflightMutex.Lock()
	defer t.inflightMutex.Unlock()
	t.inflightBytes += size
}

func (t *MemoryThrottler) ReduceInflight(size uint64) {
	t.inflightMutex.Lock()
	defer t.inflightMutex.Unlock()
	t.inflightBytes = t.inflightBytes - size
}

func (t *MemoryThrottler) InflightBytes() uint64 {
	t.inflightMutex.RLock()
	defer t.inflightMutex.RUnlock()
	return t.inflightBytes
}

func (t *MemoryThrottler) WaitLimits(
	stopCh <-chan struct{},
	parseQDone <-chan struct{},
	logger log.Logger,
) {
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.Reset()
	backoffTimer.MaxElapsedTime = 0
	nextLogDuration := backoffTimer.NextBackOff()
	logTime := time.Now()

	for t.ExceededLimits() {
		select {
		case <-stopCh:
			logger.Warn("source stopped, exit throttle wait")
			return
		case <-parseQDone:
			logger.Warn("parse queue stopped, exit throttle wait")
			return
		default:
		}

		if time.Since(logTime) > nextLogDuration {
			logTime = time.Now()
			nextLogDuration = backoffTimer.NextBackOff()
			logger.Infof(
				"reader throttled for %v, limits: %v / %v",
				backoffTimer.GetElapsedTime(),
				format.SizeUInt64(t.InflightBytes()),
				format.SizeUInt64(t.BufferSize),
			)
		}
		time.Sleep(time.Millisecond * 20)
	}
}

func NewMemoryThrottler(bufferSize uint64) Throttler {
	return &MemoryThrottler{
		BufferSize:    bufferSize,
		inflightMutex: sync.RWMutex{},
		inflightBytes: 0,
	}
}
