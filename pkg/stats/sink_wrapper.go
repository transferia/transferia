package stats

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type WrapperStats struct {
	registry          metrics.Registry
	Lag               metrics.Timer
	MaxLag            metrics.Gauge
	Timer             metrics.Timer
	RowEventsPushed   metrics.Counter
	ChangeItemsPushed metrics.Counter
	MaxReadLag        metrics.Gauge
}

var sinkerBuckets = metrics.NewDurationBuckets(
	100*time.Millisecond,
	500*time.Millisecond,
	time.Second,
	2*time.Second,
	3*time.Second,
	5*time.Second,
	7*time.Second,
	10*time.Second,
	15*time.Second,
	30*time.Second,
	45*time.Second,
	time.Minute,
	15*time.Minute,
	30*time.Minute,
	time.Hour,
	4*time.Hour,
	12*time.Hour,
	48*time.Hour,
)

func NewWrapperStats(registry metrics.Registry) *WrapperStats {
	return &WrapperStats{
		registry:          registry,
		Lag:               registry.DurationHistogram("sinker.pusher.time.row_lag_sec", sinkerBuckets),
		MaxLag:            registry.Gauge("sinker.pusher.time.row_max_lag_sec"),
		MaxReadLag:        registry.Gauge("sinker.pusher.time.row_max_read_lag_sec"),
		Timer:             registry.DurationHistogram("sinker.pusher.time.batch_push_distribution_sec", sinkerBuckets),
		RowEventsPushed:   registry.Counter("sinker.pusher.data.row_events_pushed"),
		ChangeItemsPushed: registry.Counter("sinker.pusher.data.changeitems"),
	}
}

func (s *WrapperStats) LogMaxReadLag(logger log.Logger, input []abstract.ChangeItem) {
	oldestRow, _, _, _ := batchStats(logger, input)
	if !oldestRow.IsZero() {
		s.MaxReadLag.Set(time.Since(oldestRow).Seconds())
	}
}

func (s *WrapperStats) Log(logger log.Logger, startTime time.Time, input []abstract.ChangeItem, isDebugLog bool) {
	oldestRow, freshestRow, dataRowEvents, inflighBytes := batchStats(logger, input)
	for _, row := range input {
		if row.IsRowEvent() {
			s.Lag.RecordDuration(time.Since(time.Unix(0, int64(row.CommitTime))))
		}
	}
	s.ChangeItemsPushed.Add(int64(len(input)))
	s.RowEventsPushed.Add(dataRowEvents)
	if !oldestRow.IsZero() {
		maxLag := time.Since(oldestRow)
		if maxLag.Seconds() > 0 {
			s.MaxLag.Set(maxLag.Seconds())
		}
	}
	s.Timer.RecordDuration(time.Since(startTime))
	logLine := fmt.Sprintf("Sink Committed %v row events (%v data row events, inflight: %s) in %v with %v - %v Lag. Catch up lag: %v in %v",
		len(input),
		dataRowEvents,
		humanize.Bytes(inflighBytes),
		time.Since(startTime),
		time.Since(oldestRow),
		time.Since(freshestRow),
		freshestRow.Sub(oldestRow)-time.Since(startTime),
		time.Since(startTime),
	)
	if isDebugLog {
		logger.Debug(
			logLine,
			log.Any("events", len(input)),
			log.Any("data_row_events", dataRowEvents),
			log.Any("lag", time.Since(freshestRow).Seconds()),
		)
	} else {
		logger.Info(
			logLine,
			log.Any("events", len(input)),
			log.Any("data_row_events", dataRowEvents),
			log.Any("lag", time.Since(freshestRow).Seconds()),
		)
	}
}
