// Package metrics provides interface collecting performance metrics.
package metrics

import (
	"fmt"
	"strings"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	core_metrics_prometheus "github.com/transferia/transferia/library/go/core/metrics/prometheus"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
)

const (
	SolomonMetricsChunkSize = 8000 // 10k is max as per docs: https://docs.yandex-team.ru/solomon/concepts/limits#quotas
)

func NewRegistry() core_metrics.Registry {
	return solomon.NewRegistry(solomon.NewRegistryOpts().SetUseNameTag(true))
}

func NewPrometheusRegistryWithNameProcessor() (*core_metrics_prometheus.Registry, core_metrics.Registry) {
	pr := core_metrics_prometheus.NewRegistry(core_metrics_prometheus.NewRegistryOpts())
	m := WithNameProcessor(pr, func(s string) string {
		return strings.ReplaceAll(s, ".", "_")
	})
	return pr, m
}

func NewDefaultPrometheusRegistry() core_metrics.Registry {
	_, m := NewPrometheusRegistryWithNameProcessor()
	return m
}

func WithNameProcessor(registry core_metrics.Registry, nameProcessor func(string) string) core_metrics.Registry {
	return &registryWrapper{
		nameProcessor: nameProcessor,
		registry:      registry,
	}
}

type RegistryWrapper interface {
	Registry() core_metrics.Registry
}

type registryWrapper struct {
	nameProcessor func(string) string
	registry      core_metrics.Registry
}

var _ core_metrics.GaugeWithInitialValueRegistry = (*registryWrapper)(nil)

func (r registryWrapper) Registry() core_metrics.Registry {
	return r.registry
}

func (r registryWrapper) WithTags(tags map[string]string) core_metrics.Registry {
	patchedTags := map[string]string{}
	for k, v := range tags {
		patchedTags[r.nameProcessor(k)] = r.nameProcessor(v)
	}
	return registryWrapper{
		nameProcessor: r.nameProcessor,
		registry:      r.registry.WithTags(RefineTags(patchedTags)),
	}
}

func RefineTags(tags map[string]string) map[string]string {
	patchedTags := map[string]string{}
	for l, v := range tags {
		patchedTags[l] = refineValue(v)
	}
	return patchedTags
}

func (r registryWrapper) WithPrefix(prefix string) core_metrics.Registry {
	return registryWrapper{
		nameProcessor: r.nameProcessor,
		registry:      r.registry.WithPrefix(r.nameProcessor(prefix)),
	}
}

func (r registryWrapper) ComposeName(parts ...string) string {
	return strings.Join(parts, ".")
}

func (r registryWrapper) Counter(name string) core_metrics.Counter {
	return r.registry.Counter(r.nameProcessor(name))
}

func (r registryWrapper) CounterVec(name string, labels []string) core_metrics.CounterVec {
	for i := range labels {
		labels[i] = r.nameProcessor(labels[i])
	}
	return r.registry.CounterVec(r.nameProcessor(name), labels)
}

func (r registryWrapper) FuncCounter(name string, function func() int64) core_metrics.FuncCounter {
	return r.registry.FuncCounter(r.nameProcessor(name), function)
}

func (r registryWrapper) Gauge(name string) core_metrics.Gauge {
	return r.registry.Gauge(r.nameProcessor(name))
}

func (r registryWrapper) GaugeWithInitialValue(name string, initialValue float64) core_metrics.Gauge {
	registry, ok := r.registry.(core_metrics.GaugeWithInitialValueRegistry)
	if !ok {
		panic(fmt.Sprintf("metrics: registry %T does not support GaugeWithInitialValue", r.registry))
	}
	return registry.GaugeWithInitialValue(r.nameProcessor(name), initialValue)
}

func (r registryWrapper) GaugeVec(name string, labels []string) core_metrics.GaugeVec {
	for i := range labels {
		labels[i] = r.nameProcessor(labels[i])
	}
	return r.registry.GaugeVec(r.nameProcessor(name), labels)
}

func (r registryWrapper) FuncGauge(name string, function func() float64) core_metrics.FuncGauge {
	return r.registry.FuncGauge(r.nameProcessor(name), function)
}

func (r registryWrapper) IntGauge(name string) core_metrics.IntGauge {
	return r.registry.IntGauge(r.nameProcessor(name))
}

func (r registryWrapper) IntGaugeVec(name string, labels []string) core_metrics.IntGaugeVec {
	for i := range labels {
		labels[i] = r.nameProcessor(labels[i])
	}
	return r.registry.IntGaugeVec(r.nameProcessor(name), labels)
}

func (r registryWrapper) FuncIntGauge(name string, function func() int64) core_metrics.FuncIntGauge {
	return r.registry.FuncIntGauge(r.nameProcessor(name), function)
}

func (r registryWrapper) Timer(name string) core_metrics.Timer {
	return r.registry.Timer(r.nameProcessor(name))
}

func (r registryWrapper) TimerVec(name string, labels []string) core_metrics.TimerVec {
	for i := range labels {
		labels[i] = r.nameProcessor(labels[i])
	}
	return r.registry.TimerVec(r.nameProcessor(name), labels)
}

func (r registryWrapper) Histogram(name string, buckets core_metrics.Buckets) core_metrics.Histogram {
	return r.registry.Histogram(r.nameProcessor(name), buckets)
}

func (r registryWrapper) HistogramVec(name string, buckets core_metrics.Buckets, labels []string) core_metrics.HistogramVec {
	for i := range labels {
		labels[i] = r.nameProcessor(labels[i])
	}
	return r.registry.HistogramVec(r.nameProcessor(name), buckets, labels)
}

func (r registryWrapper) DurationHistogram(name string, buckets core_metrics.DurationBuckets) core_metrics.Timer {
	return r.registry.DurationHistogram(r.nameProcessor(name), buckets)
}

func (r registryWrapper) DurationHistogramVec(name string, buckets core_metrics.DurationBuckets, labels []string) core_metrics.TimerVec {
	return r.registry.DurationHistogramVec(r.nameProcessor(name), buckets, labels)
}

// DTSUPPORT-1013
func refineValue(original string) string {
	filteredCharacters := []rune(original)
	reqLen := len(filteredCharacters)
	if reqLen > 200 {
		reqLen = 200
	} else if reqLen == 0 {
		return "-"
	}
	return string(filteredCharacters[:reqLen])
}
