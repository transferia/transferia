package stats

import (
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
)

const LabelRuntimeStatus = "runtime_status"

type PoolStats struct {
	DroppedTransfers metrics.Counter

	RunningOperations                  metrics.IntGauge
	StartedOperations                  metrics.Counter
	FailedInitOperationsServer         metrics.Counter
	FailedInitOperationsUser           metrics.Counter
	FailedStopTransfers                metrics.Counter
	FailedStopOperations               metrics.Counter
	PendingOperations                  metrics.IntGauge
	StaleTransfers                     metrics.Gauge
	TransferLastPingTime               metrics.Timer
	TransferErrors                     metrics.Counter
	OperationLastPingTime              metrics.Timer
	StaleOperations                    metrics.Gauge
	StatusCount                        map[string]metrics.Gauge
	OperationStartLatency              metrics.Timer
	TransfersCount                     metrics.IntGaugeVec
	RegularSnapshotPlanIterations      metrics.Counter
	RegularSnapshotPlanSuccess         metrics.Counter
	RegularSnapshotPlanErrors          metrics.Counter
	RegularSnapshotTasksScheduled      metrics.Counter
	RegularSnapshotTasksScheduleErrors metrics.Counter

	pm                                metrics.Registry
	operationScheduleRoundStartTime   atomic.Value // time.Time
	regularSnapshotIterationStartTime atomic.Value // time.Time
}

func (s PoolStats) SetStatusCount(status string, count int) {
	_, ok := s.StatusCount[status]
	if !ok {
		s.StatusCount[status] = s.pm.WithTags(map[string]string{
			"status": string(status),
		}).Gauge("transfers.status")
	}
	s.StatusCount[status].Set(float64(count))
}

var operationStartDurationBuckets = metrics.NewDurationBuckets(
	10*time.Millisecond,
	50*time.Millisecond,
	100*time.Millisecond,
	200*time.Millisecond,
	500*time.Millisecond,
	1*time.Second,
	2*time.Second,
	5*time.Second,
	10*time.Second,
	30*time.Second,
	1*time.Minute,
	2*time.Minute,
	5*time.Minute,
	10*time.Minute,
)

func NewPoolStats(mtrc metrics.Registry) *PoolStats {
	pm := mtrc.WithTags(map[string]string{
		"component": "pool",
	})

	stats := &PoolStats{
		DroppedTransfers:                   pm.Counter("transfers.dropped"),
		RunningOperations:                  pm.IntGauge("operations.running"),
		StartedOperations:                  pm.Counter("operations.started"),
		FailedInitOperationsServer:         pm.Counter("operations.failed_init.server"),
		FailedInitOperationsUser:           pm.Counter("operations.failed_init.user"),
		FailedStopTransfers:                pm.Counter("transfers.failed_stop"),
		FailedStopOperations:               pm.Counter("operations.failed_stop"),
		PendingOperations:                  pm.IntGauge("operations.pending"),
		StaleTransfers:                     pm.Gauge("transfers.stale"),
		TransferLastPingTime:               pm.Timer("operations.ping_delay"),
		TransferErrors:                     pm.Counter("transfer.total_retries"),
		OperationLastPingTime:              pm.Timer("operations.ping_delay"),
		StaleOperations:                    pm.Gauge("operations.stale"),
		StatusCount:                        map[string]metrics.Gauge{},
		OperationStartLatency:              pm.DurationHistogram("operations.start_latency", operationStartDurationBuckets),
		TransfersCount:                     pm.IntGaugeVec("transfers.count", []string{LabelRuntimeStatus}),
		RegularSnapshotPlanIterations:      pm.Counter("regular_snapshots.iterations"),
		RegularSnapshotPlanSuccess:         pm.Counter("regular_snapshots.success"),
		RegularSnapshotPlanErrors:          pm.Counter("regular_snapshots.errors"),
		RegularSnapshotTasksScheduled:      pm.Counter("regular_snapshots.tasks.scheduled"),
		RegularSnapshotTasksScheduleErrors: pm.Counter("regular_snapshots.tasks.errors"),

		pm:                                pm,
		operationScheduleRoundStartTime:   atomic.Value{},
		regularSnapshotIterationStartTime: atomic.Value{},
	}
	pm.FuncGauge("operations.iteration_duration", stats.currentOperationScheduleDuration)
	pm.FuncGauge("regular_snapshots.iteration_duration", stats.currentRegularSnapshotScheduleDuration)
	return stats
}

func (s *PoolStats) OperationScheduleRoundStarted(now time.Time) {
	s.operationScheduleRoundStartTime.Store(now)
}

func (s *PoolStats) OperationScheduleRoundFinished() {
	s.operationScheduleRoundStartTime.Store(time.Time{})
}

func (s *PoolStats) currentOperationScheduleDuration() float64 {
	startTime, ok := s.operationScheduleRoundStartTime.Load().(time.Time)
	if !ok {
		// OperationScheduleRoundStarted() has not been called yet
		return 0
	}
	if startTime.IsZero() {
		// Iteration is not in progress
		return 0
	}
	return time.Since(startTime).Seconds()
}

func (s *PoolStats) RegularSnapshotScheduleStarted(now time.Time) {
	s.regularSnapshotIterationStartTime.Store(now)
}

func (s *PoolStats) RegularSnapshotScheduleFinished() {
	s.regularSnapshotIterationStartTime.Store(time.Time{})
}

func (s *PoolStats) currentRegularSnapshotScheduleDuration() float64 {
	startTime, ok := s.regularSnapshotIterationStartTime.Load().(time.Time)
	if !ok {
		// RegularSnapshotScheduleStarted() has not been called yet
		return 0
	}
	if startTime.IsZero() {
		// Iteration is not in progress
		return 0
	}
	return time.Since(startTime).Seconds()
}
