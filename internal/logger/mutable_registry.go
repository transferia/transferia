package logger

import (
	"fmt"
	"sync"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
)

// mutableRegistry is a nasty hack, try not to use it. It overrides some metric
// types which ignore records before tags are set.
type mutableRegistry struct {
	core_metrics.Registry
	metrics []mutableMetric
	mutex   sync.Mutex // just in case
}

var _ core_metrics.GaugeWithInitialValueRegistry = (*mutableRegistry)(nil)

func NewMutableRegistry(registry core_metrics.Registry) core_metrics.Registry {
	return &mutableRegistry{
		Registry: registry,
		metrics:  nil,
		mutex:    sync.Mutex{},
	}
}

func (r *mutableRegistry) WithTags(tags map[string]string) core_metrics.Registry {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.Registry = r.Registry.WithTags(tags)
	for _, metric := range r.metrics {
		metric.Init(r.Registry)
	}
	return r.Registry
}

func (r *mutableRegistry) GaugeWithInitialValue(name string, initialValue float64) core_metrics.Gauge {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	registry, ok := r.Registry.(core_metrics.GaugeWithInitialValueRegistry)
	if !ok {
		panic(fmt.Sprintf("metrics: registry %T does not support GaugeWithInitialValue", r.Registry))
	}
	return registry.GaugeWithInitialValue(name, initialValue)
}

func (r *mutableRegistry) Counter(name string) core_metrics.Counter {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	counter := newMutableCounter(name)
	r.metrics = append(r.metrics, counter)
	return counter
}

func (r *mutableRegistry) Histogram(name string, buckets core_metrics.Buckets) core_metrics.Histogram {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	histogram := newMutableHistogram(name, buckets)
	r.metrics = append(r.metrics, histogram)
	return histogram
}

//---------------------------------------------------------------------------------------------------------------------
// interface 'mutableMetric'

type mutableMetric interface {
	Init(registry core_metrics.Registry)
}

//---------------------------------------------------------------------------------------------------------------------
// 1st implementation of 'mutableMetric' - counter

type mutableCounter struct {
	name    string
	counter core_metrics.Counter
}

func (c *mutableCounter) Init(registry core_metrics.Registry) {
	c.counter = registry.Counter(c.name)
}

func (c *mutableCounter) Inc() {
	if c.counter != nil {
		c.counter.Inc()
	}
}

func (c *mutableCounter) Add(delta int64) {
	if c.counter != nil {
		c.counter.Add(delta)
	}
}

func newMutableCounter(name string) *mutableCounter {
	return &mutableCounter{
		name:    name,
		counter: nil,
	}
}

//---------------------------------------------------------------------------------------------------------------------
// 2nd implementation of 'mutableMetric' - histogram

type mutableHistogram struct {
	name      string
	buckets   core_metrics.Buckets
	histogram core_metrics.Histogram
}

func (h *mutableHistogram) Init(registry core_metrics.Registry) {
	h.histogram = registry.Histogram(h.name, h.buckets)
}

func (h *mutableHistogram) RecordValue(value float64) {
	if h.histogram != nil {
		h.histogram.RecordValue(value)
	}
}

func newMutableHistogram(name string, buckets core_metrics.Buckets) *mutableHistogram {
	return &mutableHistogram{
		name:      name,
		buckets:   buckets,
		histogram: nil,
	}
}
