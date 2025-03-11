package stats

import (
	"github.com/transferia/transferia/library/go/core/metrics"
)

type PoolStats struct {
	RunningTransfers     metrics.Gauge
	DroppedTransfers     metrics.Counter
	NeedRestartTransfers metrics.Counter

	RunningOperations     metrics.Gauge
	FailedInitOperations  metrics.Counter
	PendingOperations     metrics.Gauge
	StaleTransfers        metrics.Gauge
	TaskScheduleTime      metrics.Timer
	TransferLastPingTime  metrics.Timer
	TransferErrors        metrics.Counter
	OperationLastPingTime metrics.Timer
	StaleOperations       metrics.Gauge
	StatusCount           map[string]metrics.Gauge
	pm                    metrics.Registry
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

func NewPoolStats(mtrc metrics.Registry) *PoolStats {
	pm := mtrc.WithTags(map[string]string{
		"component": "pool",
	})
	return &PoolStats{
		RunningTransfers:      pm.Gauge("transfers.running"),
		DroppedTransfers:      pm.Counter("transfers.dropped"),
		NeedRestartTransfers:  pm.Counter("transfers.need_restart"),
		RunningOperations:     pm.Gauge("operations.running"),
		FailedInitOperations:  pm.Counter("operations.failed_init"),
		PendingOperations:     pm.Gauge("operations.pending"),
		StaleTransfers:        pm.Gauge("transfers.stale"),
		TaskScheduleTime:      pm.Timer("operations.schedule_time"),
		TransferLastPingTime:  pm.Timer("operations.ping_delay"),
		TransferErrors:        pm.Counter("transfer.total_retries"),
		OperationLastPingTime: pm.Timer("operations.ping_delay"),
		StaleOperations:       pm.Gauge("operations.stale"),
		StatusCount:           map[string]metrics.Gauge{},
		pm:                    pm,
	}
}
