package stats

import (
	"fmt"
	"sync"

	"github.com/transferria/transferria/library/go/core/metrics"
)

type ChStats struct {
	Len        metrics.Counter
	Count      metrics.Counter
	Size       metrics.Counter
	registry   metrics.Registry
	hostGauges map[string]metrics.Gauge
	rw         sync.Mutex
}

// timmyb32r - very strange source file.
// it's clickhouse-sinker specific stuff - but it uses on multiple levels:
// - cluster
// - server
// - shard
// - table
// TODO - do something with it! it's wrong!

func (s *ChStats) HostGauge(host string, metric string) metrics.Gauge {
	s.rw.Lock()
	defer s.rw.Unlock()
	p := fmt.Sprintf("task.replication.%v.%v", metric, host)
	if _, ok := s.hostGauges[p]; !ok {
		s.hostGauges[p] = s.registry.Gauge(p)
	}
	return s.hostGauges[p]
}

func NewChStats(registry metrics.Registry) *ChStats {
	return &ChStats{
		registry:   registry,
		hostGauges: map[string]metrics.Gauge{},
		rw:         sync.Mutex{},
		Len:        registry.Counter("task.replication.upload.rows"),
		Count:      registry.Counter("task.replication.upload.transactions"),
		Size:       registry.Counter("task.replication.upload.bytes"),
	}
}
