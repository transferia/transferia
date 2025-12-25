package middlewares

import (
	"sync"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

// Statistician tracks the traffic of sinker
func Statistician(logger log.Logger, stats *stats.WrapperStats) func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newStatistician(s, logger, stats)
	}
}

type statistician struct {
	sink abstract.Sinker

	stats         *stats.WrapperStats
	statsMtx      sync.Mutex
	ticker        *time.Ticker
	stopMonitorCh chan struct{}

	NoLagForTooLongThreshold time.Duration

	logger log.Logger
}

func newStatistician(s abstract.Sinker, logger log.Logger, stats *stats.WrapperStats) *statistician {
	result := &statistician{
		sink: s,

		stats:         stats,
		statsMtx:      sync.Mutex{},
		ticker:        time.NewTicker(3 * time.Hour), // base interval should be greater than the time required to start the slowest-starting transfer
		stopMonitorCh: make(chan struct{}),

		NoLagForTooLongThreshold: 15 * time.Minute,

		logger: logger,
	}
	go result.monitorSuspicioslyMissingPushes()
	return result
}

func (s *statistician) Close() error {
	s.ticker.Stop()
	close(s.stopMonitorCh)
	return s.sink.Close()
}

func (s *statistician) Push(input []abstract.ChangeItem) error {
	startTime := time.Now()
	s.stats.LogMaxReadLag(s.logger, input)
	err := s.sink.Push(input)
	s.ticker.Reset(s.NoLagForTooLongThreshold)
	if err == nil {
		func() {
			s.statsMtx.Lock()
			defer s.statsMtx.Unlock()
			s.stats.Log(s.logger, startTime, input, false)
		}()
	}
	return err
}

func (s *statistician) monitorSuspicioslyMissingPushes() {
	for {
		select {
		case <-s.stopMonitorCh:
			return
		case <-s.ticker.C:
		}
		s.logger.Debug("Too long no updates, reset Lag counter")
		func() {
			s.statsMtx.Lock()
			defer s.statsMtx.Unlock()
			s.stats.MaxLag.Set(0)
		}()
	}
}
