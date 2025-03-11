package stats

import (
	"sync"

	"github.com/transferria/transferria/library/go/core/metrics"
)

var cache = map[string]*TableStat{}
var tableStatLock = sync.Mutex{}

type TableStat struct {
	Source   metrics.Gauge
	Target   metrics.Gauge
	Diff     metrics.Gauge
	Metrics  map[string]metrics.Counter
	registry metrics.Registry
}

func NewTableMetrics(registry metrics.Registry, table string) *TableStat {
	tableStatLock.Lock()
	defer tableStatLock.Unlock()
	if t, ok := cache[table]; ok {
		return t
	}
	subRegistry := registry.WithTags(map[string]string{"table": table})
	cache[table] = &TableStat{
		Source:   subRegistry.Gauge("storage.source_rows"),
		Target:   subRegistry.Gauge("storage.target_rows"),
		Diff:     subRegistry.Gauge("storage.diff_perc"),
		Metrics:  map[string]metrics.Counter{},
		registry: registry,
	}
	return cache[table]
}
