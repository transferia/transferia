package batching_logger

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
)

// entry is a count of messages and the lastTime time they were logged
type entry struct {
	count    int
	lastTime time.Time
}

// classification is a level and key of the message
type classification struct {
	level levelKind
	key   string
}

// aggregatorOptions is a set of options for the spam aggregator
type aggregatorConfig struct {
	flushInterval time.Duration
	threshold     int
	maxKeys       int
}

func defaultAggregatorConfig() aggregatorConfig {
	return aggregatorConfig{
		flushInterval: 1 * time.Minute,
		threshold:     32,
		maxKeys:       1024,
	}
}

// spamAggregator aggregates messages by level and key
type spamAggregator struct {
	logger log.Logger
	mu     sync.Mutex
	counts map[classification]*entry
	ticker *time.Ticker
	config aggregatorConfig

	willBeFlushed atomic.Bool // should be true olny if goroutine with waitFlush is started, false otherwise
}

func newSpamAggregator(logger log.Logger, config aggregatorConfig) *spamAggregator {
	ag := &spamAggregator{
		logger:        logger,
		counts:        make(map[classification]*entry),
		ticker:        time.NewTicker(config.flushInterval),
		config:        config,
		willBeFlushed: atomic.Bool{},
		mu:            sync.Mutex{},
	}
	ag.willBeFlushed.Store(false)
	ag.waitFlush()

	return ag
}

// start flush after flushInterval
// if flush wait goroutine is already started, do nothing
func (a *spamAggregator) waitFlush() {
	if !a.willBeFlushed.CompareAndSwap(false, true) {
		return
	}
	go func() {
		time.Sleep(a.config.flushInterval)
		a.flush()
	}()
}

// flushes the spam aggregator and sends the summarized messages to the logger
// if log is enough old, it will be deleted from the map
// else it will be reset to 0
func (a *spamAggregator) flush() {
	now := time.Now()
	a.mu.Lock()
	for k, e := range a.counts {
		isOldEnough := now.Sub(e.lastTime) > 2*a.config.flushInterval
		a.suppressLog(k, e)
		if isOldEnough {
			delete(a.counts, k)
		} else {
			e.count = min(a.config.threshold, e.count)
			e.lastTime = time.Unix(0, 0)
		}
	}
	a.willBeFlushed.Store(false)
	a.mu.Unlock()
}

func (a *spamAggregator) suppressLog(cls classification, entry *entry) {
	suppressed := entry.count - a.config.threshold
	if suppressed <= 0 {
		return
	}
	msg := fmt.Sprintf("got %d messages: %s", suppressed, cls.key)
	switch cls.level {
	case levelTrace:
		a.logger.Trace(msg)
	case levelDebug:
		a.logger.Debug(msg)
	case levelInfo:
		a.logger.Info(msg)
	case levelWarn:
		a.logger.Warn(msg)
	case levelError:
		a.logger.Error(msg)
	case levelFatal:
		a.logger.Error(msg)
	}
}

// shouldLog increments counter for (level, key) and returns true if this occurrence
// should be logged immediately. Only lastTime `threshold` occurrences per window are
// passed through; the rest will be summarized on flush.
func (a *spamAggregator) shouldLog(level levelKind, key string) bool {
	now := time.Now()
	cls := classification{level: level, key: key}
	a.mu.Lock()
	defer func() {
		a.waitFlush()
		a.mu.Unlock()
	}()
	if e, ok := a.counts[cls]; ok {
		e.count++
		e.lastTime = now
		return e.count <= a.config.threshold
	}
	// if maxKeys is reached, delete the oldest key
	if len(a.counts) >= a.config.maxKeys {
		var oldestKey classification
		lastTime := now
		for k, v := range a.counts {
			if v.lastTime.Before(lastTime) {
				lastTime = v.lastTime
				oldestKey = k
			}
		}
		a.suppressLog(oldestKey, a.counts[oldestKey])
		delete(a.counts, oldestKey)
	}
	a.counts[cls] = &entry{count: 1, lastTime: now}
	return true
}
