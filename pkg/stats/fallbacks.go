package stats

import "github.com/transferia/transferia/library/go/core/metrics"

type FallbackStats struct {
	registry metrics.Registry

	// Items counts the number of items to which fallbacks chain was applied
	Items metrics.Counter
	// Deepness tracks the number of fallbacks applied
	Deepness metrics.Gauge
	// Errors counts the number of errorneous fallbacks
	Errors metrics.Counter
}

// FallbackStatsCombination is an object unifying stats for source and target fallbacks
type FallbackStatsCombination struct {
	registry metrics.Registry

	Source *FallbackStats
	Target *FallbackStats
}

func NewFallbackStatsCombination(registry metrics.Registry) *FallbackStatsCombination {
	return &FallbackStatsCombination{
		registry: registry,

		Source: &FallbackStats{
			registry: registry,

			Items:    registry.Counter("fallbacks.source.items"),
			Deepness: registry.Gauge("fallbacks.source.deepness"),
			Errors:   registry.Counter("fallbacks.source.errors"),
		},
		Target: &FallbackStats{
			registry: registry,

			Items:    registry.Counter("fallbacks.target.items"),
			Deepness: registry.Gauge("fallbacks.target.deepness"),
			Errors:   registry.Counter("fallbacks.target.errors"),
		},
	}
}
