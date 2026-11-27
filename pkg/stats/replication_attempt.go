package stats

import "github.com/transferia/transferia/library/go/core/metrics"

// ReplicationAttemptMetrics shares instrumentation across the sources and sinks of one attempt.
type ReplicationAttemptMetrics struct {
	onSuccessfulDataPush func()
}

func NewReplicationAttemptMetrics(registry metrics.Registry) *ReplicationAttemptMetrics {
	return &ReplicationAttemptMetrics{onSuccessfulDataPush: nil}
}

func (m *ReplicationAttemptMetrics) BuildSourceStats(registry metrics.Registry) *SourceStats {
	sourceStats := NewSourceStats(registry)
	sourceStats.attempt = m
	return sourceStats
}

func (m *ReplicationAttemptMetrics) Start() func() {
	return func() {}
}

// RecordSuccessfulDataPush notifies attempt metrics when the sink has written a batch of data.
func (m *ReplicationAttemptMetrics) RecordSuccessfulDataPush() {
	if m != nil && m.onSuccessfulDataPush != nil {
		m.onSuccessfulDataPush()
	}
}
