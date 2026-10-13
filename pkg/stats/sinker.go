package stats

import (
	"fmt"
	"sync"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
)

type SinkerStats struct {
	Inflight  core_metrics.Counter
	Elapsed   core_metrics.Timer
	tableStat map[string]*TableStat
	timers    map[string]core_metrics.Timer
	registry  core_metrics.Registry
	rw        sync.Mutex
	Wal       core_metrics.Counter
}

func NewSinkerStats(registry core_metrics.Registry) *SinkerStats {
	return &SinkerStats{
		Wal:       registry.Counter("sinker.transactions.total"),
		Inflight:  registry.Counter("sinker.transactions.inflight"),
		Elapsed:   registry.Timer("sinker.time.push"),
		tableStat: map[string]*TableStat{},
		timers:    map[string]core_metrics.Timer{},
		registry:  registry,
		rw:        sync.Mutex{},
	}
}

func (s *SinkerStats) TargetRows(table string, rows uint64) {
	if abstract.IsSystemTable(table) {
		return
	}
	targetRows(table, rows)
	s.rw.Lock()
	defer s.rw.Unlock()
	if _, ok := s.tableStat[table]; !ok {
		s.tableStat[table] = NewTableMetrics(s.registry, table)
	}
	s.tableStat[table].Target.Set(float64(rows))
}

func (s *SinkerStats) Table(table string, metric string, rows int) {
	if abstract.IsSystemTable(table) {
		return
	}
	s.rw.Lock()
	defer s.rw.Unlock()
	if len(s.tableStat) > 1000 { // do no track more than solomon can handle
		return
	}
	if _, ok := s.tableStat[table]; !ok {
		s.tableStat[table] = NewTableMetrics(s.registry, table)
	}
	s.tableStat[table].counter(table, metric).Add(int64(rows))
}

func (s *SinkerStats) RecordDuration(metric string, duration time.Duration) {
	s.rw.Lock()
	defer s.rw.Unlock()
	timer, ok := s.timers[metric]
	if !ok {
		timer = s.registry.Timer(fmt.Sprintf("sinker.time.%s", metric))
		s.timers[metric] = timer
	}
	timer.RecordDuration(duration)
}
