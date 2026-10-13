package stats

import (
	"fmt"
	"sync"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
)

var cache = map[string]*TableStat{}
var tableStatLock = sync.Mutex{}

type TableStat struct {
	Source   core_metrics.Gauge
	Target   core_metrics.Gauge
	Diff     core_metrics.Gauge
	Metrics  map[string]core_metrics.Counter
	registry core_metrics.Registry
	mu       sync.Mutex
}

func NewTableMetrics(registry core_metrics.Registry, table string) *TableStat {
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
		Metrics:  map[string]core_metrics.Counter{},
		registry: registry,
		mu:       sync.Mutex{},
	}
	return cache[table]
}

// counter returns the table counter, creating it on first use. TableStat is shared by all sinks of the table.
func (t *TableStat) counter(table, metric string) core_metrics.Counter {
	t.mu.Lock()
	defer t.mu.Unlock()
	counter, ok := t.Metrics[metric]
	if !ok {
		counter = t.registry.WithTags(map[string]string{"table": table}).Counter(fmt.Sprintf("sinker.table.%v", metric))
		t.Metrics[metric] = counter
	}
	return counter
}
