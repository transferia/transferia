package logger

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/metrics"
)

// mutableRegistry is a nasty hack, try not to use it. It overrides some metric
// types which ignore records before tags are set.
type mutableRegistry struct {
	metrics.Registry
	metrics []mutableMetric
	mutex   sync.Mutex // just in case
}

func NewMutableRegistry(registry metrics.Registry) metrics.Registry {
	return &mutableRegistry{
		Registry: registry,
		metrics:  nil,
		mutex:    sync.Mutex{},
	}
}

func (r *mutableRegistry) WithTags(tags map[string]string) metrics.Registry {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.Registry = r.Registry.WithTags(tags)
	for _, metric := range r.metrics {
		metric.Init(r.Registry)
	}
	return r.Registry
}

func (r *mutableRegistry) Counter(name string) metrics.Counter {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	counter := newMutableCounter(name)
	r.metrics = append(r.metrics, counter)
	return counter
}

func (r *mutableRegistry) Histogram(name string, buckets metrics.Buckets) metrics.Histogram {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	histogram := newMutableHistogram(name, buckets)
	r.metrics = append(r.metrics, histogram)
	return histogram
}

//---------------------------------------------------------------------------------------------------------------------
// interface 'mutableMetric'

type mutableMetric interface {
	Init(registry metrics.Registry)
}

//---------------------------------------------------------------------------------------------------------------------
// 1st implementation of 'mutableMetric' - counter

type mutableCounter struct {
	name    string
	counter metrics.Counter
}

func (c *mutableCounter) Init(registry metrics.Registry) {
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
	buckets   metrics.Buckets
	histogram metrics.Histogram
}

func (h *mutableHistogram) Init(registry metrics.Registry) {
	h.histogram = registry.Histogram(h.name, h.buckets)
}

func (h *mutableHistogram) RecordValue(value float64) {
	if h.histogram != nil {
		h.histogram.RecordValue(value)
	}
}

func newMutableHistogram(name string, buckets metrics.Buckets) *mutableHistogram {
	return &mutableHistogram{
		name:      name,
		buckets:   buckets,
		histogram: nil,
	}
}
