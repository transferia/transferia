package stats

import "github.com/transferia/transferia/library/go/core/metrics"

type ReplicationStats struct {
	StartUnix metrics.Gauge
	Running   metrics.Gauge
}

func NewReplicationStats(registry metrics.Registry) *ReplicationStats {
	return &ReplicationStats{
		StartUnix: registry.Gauge("replication.start.unix"),
		Running:   registry.Gauge("replication.running"),
	}
}
