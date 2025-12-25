package middlewares

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

// IntervalThrottler blocks Push until a given interval since the finish of the previous Push passes
func IntervalThrottler(logger log.Logger, interval time.Duration) func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newIntervalThrottler(s, logger, interval)
	}
}

type intervalThrottler struct {
	sink abstract.Sinker

	Interval time.Duration

	lastPushFinish time.Time

	logger log.Logger
}

func newIntervalThrottler(s abstract.Sinker, logger log.Logger, interval time.Duration) *intervalThrottler {
	return &intervalThrottler{
		sink: s,

		Interval: interval,

		lastPushFinish: time.Time{},

		logger: logger,
	}
}

func (t *intervalThrottler) Close() error {
	return t.sink.Close()
}

func (t *intervalThrottler) Push(items []abstract.ChangeItem) error {
	durationToWait := t.calculateDurationToWait()
	if durationToWait > 0 {
		t.logger.Info("Throttling Push by interval", log.String("to_wait", durationToWait.String()), log.String("interval", t.Interval.String()))
		// recalculate duration to wait in case the logging call took extra long
		time.Sleep(t.calculateDurationToWait())
	}
	result := t.sink.Push(items)
	t.lastPushFinish = time.Now()
	return result
}

func (t *intervalThrottler) calculateDurationToWait() time.Duration {
	return t.Interval - time.Since(t.lastPushFinish)
}
