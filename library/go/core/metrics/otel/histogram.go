package otel

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var _ metrics.Histogram = (*Histogram)(nil)
var _ Metric = (*Histogram)(nil)

type Histogram struct {
	hist metric.Float64Histogram
	tags attribute.Set
}

func (h *Histogram) RecordValue(value float64) {
	h.hist.Record(context.Background(), value, metric.WithAttributeSet(h.tags))
}

func (h *Histogram) RecordDuration(value time.Duration) {
	h.RecordValue(value.Seconds())
}

func (h *Histogram) otelMetric() {}
