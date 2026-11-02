package stats

import (
	"sync"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
)

// lazyLag publishes no samples until its first measurement. The gauge is
// registered with that value so collection cannot see an intermediate zero.
type lazyLag struct {
	mu    sync.Mutex
	gauge core_metrics.Gauge
}

func newLazyLag() *lazyLag {
	return &lazyLag{
		mu:    sync.Mutex{},
		gauge: nil,
	}
}

func (g *lazyLag) set(registry core_metrics.GaugeWithInitialValueRegistry, name string, value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.gauge == nil {
		g.gauge = registry.GaugeWithInitialValue(name, value)
	}
	// Registration may have returned a gauge from a previous pipeline. Update
	// that shared gauge too, rather than relying on the initial value argument.
	g.gauge.Set(value)
}
