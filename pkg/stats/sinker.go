package stats

import (
	"fmt"
	"sync"
	"time"

	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/pkg/abstract"
)

type SinkerStats struct {
	Inflight  metrics.Counter
	Elapsed   metrics.Timer
	tableStat map[string]*TableStat
	timers    map[string]metrics.Timer
	registry  metrics.Registry
	rw        sync.Mutex
	Wal       metrics.Counter
}

func NewSinkerStats(registry metrics.Registry) *SinkerStats {
	return &SinkerStats{
		Wal:       registry.Counter("sinker.transactions.total"),
		Inflight:  registry.Counter("sinker.transactions.inflight"),
		Elapsed:   registry.Timer("sinker.time.push"),
		tableStat: map[string]*TableStat{},
		timers:    map[string]metrics.Timer{},
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
	p := fmt.Sprintf("sinker.table.%v", metric) // sinker.table ??
	if _, ok := s.tableStat[table].Metrics[metric]; !ok {
		s.tableStat[table].Metrics[metric] = s.tableStat[table].registry.WithTags(map[string]string{"table": table}).Counter(p)
	}
	s.tableStat[table].Metrics[metric].Add(int64(rows))
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
