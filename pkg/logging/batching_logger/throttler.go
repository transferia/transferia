package batching_logger

import (
	"sync"
	"time"
)

type Throttler interface {
	// TryLog atomically checks whether logging is allowed and, if so,
	// marks the throttler state. Returns true if logging should proceed.
	TryLog() bool
	// IsSilent reports whether the "skipped by throttler" diagnostic line
	// should be suppressed when TryLog() returns false.
	IsSilent() bool
}

// absent

type AbsentThrottler struct{}

func (t *AbsentThrottler) TryLog() bool {
	return true
}

func (t *AbsentThrottler) IsSilent() bool {
	return false
}

func NewAbsentThrottler() *AbsentThrottler {
	return &AbsentThrottler{}
}

// once

type OnceThrottler struct {
	isAlreadyLogged bool
}

func (t *OnceThrottler) TryLog() bool {
	if !t.isAlreadyLogged {
		t.isAlreadyLogged = true
		return true
	}
	return false
}

func (t *OnceThrottler) IsSilent() bool {
	return false
}

func NewOnceThrottler() *OnceThrottler {
	return &OnceThrottler{
		isAlreadyLogged: false,
	}
}

// interval

type IntervalThrottler struct {
	interval   time.Duration
	lastLogged time.Time
	nowFunc    func() time.Time
}

func (t *IntervalThrottler) TryLog() bool {
	now := t.nowFunc()
	if now.Sub(t.lastLogged) >= t.interval {
		t.lastLogged = now
		return true
	}
	return false
}

func (t *IntervalThrottler) IsSilent() bool {
	return false
}

func NewIntervalThrottler(interval time.Duration) *IntervalThrottler {
	return &IntervalThrottler{
		interval:   interval,
		lastLogged: time.Time{}, // zero time — first call always allowed
		nowFunc:    time.Now,
	}
}

// concurrent (thread-safe wrapper over any throttler)

type ConcurrentThrottler struct {
	mu    sync.Mutex
	inner Throttler
}

func (t *ConcurrentThrottler) TryLog() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inner.TryLog()
}

func (t *ConcurrentThrottler) IsSilent() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inner.IsSilent()
}

func NewConcurrentThrottler(inner Throttler) *ConcurrentThrottler {
	return &ConcurrentThrottler{
		mu:    sync.Mutex{},
		inner: inner,
	}
}

// counter

type CountThrottler struct {
	limit int
	count int
}

func (t *CountThrottler) TryLog() bool {
	if t.count < t.limit {
		t.count++
		return true
	}
	return false
}

func (t *CountThrottler) IsSilent() bool {
	return false
}

func NewCountThrottler(limit int) *CountThrottler {
	return &CountThrottler{
		limit: limit,
		count: 0,
	}
}

// silent (decorator that suppresses the "skipped by throttler" diagnostic line)

type SilentThrottler struct {
	inner Throttler
}

func (t *SilentThrottler) TryLog() bool {
	return t.inner.TryLog()
}

func (t *SilentThrottler) IsSilent() bool {
	return true
}

// NewSilentThrottler wraps any throttler so that the "skipped by throttler"
// diagnostic line is not logged when TryLog() returns false.
func NewSilentThrottler(inner Throttler) *SilentThrottler {
	return &SilentThrottler{
		inner: inner,
	}
}
