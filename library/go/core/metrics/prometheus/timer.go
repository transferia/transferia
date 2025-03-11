package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/transferria/transferria/library/go/core/metrics"
)

var _ metrics.Timer = (*Timer)(nil)

// Timer measures gauge duration.
type Timer struct {
	gg prometheus.Gauge
}

func (t Timer) RecordDuration(value time.Duration) {
	t.gg.Set(value.Seconds())
}
