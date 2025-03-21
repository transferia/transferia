package stats

import "github.com/transferia/transferia/library/go/core/metrics"

type WorkerStats struct {
	OOMKilled            metrics.Gauge
	OOMKills             metrics.Counter
	RestartFailure       metrics.Gauge
	RestartFailures      metrics.Counter
	FatalRestartFailure  metrics.Gauge
	FatalRestartFailures metrics.Counter
}

func NewWorkerStats(cpRegistry metrics.Registry, dpRegistry metrics.Registry) *WorkerStats {
	return &WorkerStats{
		OOMKilled:            dpRegistry.Gauge("runtime.oom"),
		OOMKills:             cpRegistry.Counter("runtime.oom"),
		RestartFailure:       dpRegistry.Gauge("worker.failure"),
		RestartFailures:      cpRegistry.Counter("worker.failure"),
		FatalRestartFailure:  dpRegistry.Gauge("worker.failure.fatal"),
		FatalRestartFailures: cpRegistry.Counter("worker.failure.fatal"),
	}
}
