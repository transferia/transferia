package util

import (
	"context"
	"sync"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

// SmartTimer is an object that can be waited on for time to pass.
//   - Supports concurrent operations;
//   - Properly works with zero and negative duration;
//
// The timer has three states:
//   - Fired: the initial state;
//   - Paused: the timer is paused and is waiting for its start;
//   - Ticking: the timer is ticking and will become fired when the interval set passes.
type SmartTimer struct {
	alarmCh chan time.Time

	state               smartTimerState
	lastRoutineCanceler func()

	mtx sync.Mutex

	Duration time.Duration
}

type smartTimerState int

const (
	stsFired   smartTimerState = 0
	stsTicking smartTimerState = 1
	stsPaused  smartTimerState = 2
)

// NewSmartTimer constructs a timer with the given duration and puts it into "fired" state.
func NewSmartTimer(d time.Duration) *SmartTimer {
	result := &SmartTimer{
		// bufferized in order to operate on this channel under mutex
		alarmCh: make(chan time.Time, 1),

		state:               stsFired,
		lastRoutineCanceler: nil,

		mtx: sync.Mutex{},

		Duration: d,
	}
	return result
}

// Restart starts the timer with the duration set in its constructor, moving it to "ticking" state.
// If the Duration is zero, moves the timer directly to "fired" state.
// If the timer is already in "ticking" state, the method restarts the timer.
func (t *SmartTimer) Restart() {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	t.deactivateLastRoutine()

	if t.Duration <= 0 {
		t.state = stsFired
		return
	}

	t.activateRoutine()

	t.state = stsTicking
}

// C returns a channel a read from which will return when the timer goes from "ticking" to "fired" state.
func (t *SmartTimer) C() <-chan time.Time {
	return t.alarmCh
}

// HasFired returns true if the timer is in "fired" state.
func (t *SmartTimer) HasFired() bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	return t.state == stsFired
}

// Pause moves the timer to "paused" state.
func (t *SmartTimer) Pause() {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	t.deactivateLastRoutine()

	t.state = stsPaused
}

// deactivateLastRoutine deactivates the last routine run by smart timer and cleans up the alarm channel.
// This method must be run with mtx already taken
func (t *SmartTimer) deactivateLastRoutine() {
	if t.lastRoutineCanceler == nil {
		return
	}
	t.lastRoutineCanceler()
	t.lastRoutineCanceler = nil
	// ensure the channel is empty
	select {
	case <-t.alarmCh:
	default:
	}
}

func (t *SmartTimer) activateRoutine() {
	routineCtx, canceler := context.WithTimeout(context.Background(), t.Duration)
	canceled := new(bool)
	t.lastRoutineCanceler = func() {
		*canceled = true
		canceler()
	}
	go func(ctx context.Context, canceled *bool) {
		<-ctx.Done()
		if xerrors.Is(ctx.Err(), context.Canceled) {
			return
		}
		t.mtx.Lock()
		defer t.mtx.Unlock()
		if *canceled {
			return
		}
		t.alarmCh <- time.Now()
		t.state = stsFired
	}(routineCtx, canceled)
}
