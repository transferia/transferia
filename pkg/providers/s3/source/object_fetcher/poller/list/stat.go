//go:build !disable_s3_provider

package list

import (
	"time"

	"go.ytsaurus.tech/library/go/core/log"
)

type stat struct {
	startTime              time.Time
	skippedBcsNotMatched   int64
	skippedBcsNotMine      int64
	skippedBcsMineButKnown int64
	notSkipped             int64
}

func (s *stat) duration() time.Duration {
	return time.Since(s.startTime)
}

func (s *stat) log(logger log.Logger) {
	logger.Infof(
		"ListNewMyFiles finished, time_elapsed:%s, skippedBcsNotMatched:%d, skippedBcsNotMine:%d, skippedBcsMineButKnown:%d, notSkipped:%d",
		s.duration(),
		s.skippedBcsNotMatched,
		s.skippedBcsNotMine,
		s.skippedBcsMineButKnown,
		s.notSkipped,
	)
}

func newStat() *stat {
	return &stat{
		startTime:              time.Now(),
		skippedBcsNotMatched:   int64(0),
		skippedBcsNotMine:      int64(0),
		skippedBcsMineButKnown: int64(0),
		notSkipped:             int64(0),
	}
}
