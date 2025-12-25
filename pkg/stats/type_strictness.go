package stats

import (
	"github.com/transferia/transferia/library/go/core/metrics"
)

type TypeStrictnessStats struct {
	registry metrics.Registry

	Good metrics.Counter
	Bad  metrics.Counter
}

func NewTypeStrictnessStats(registry metrics.Registry) *TypeStrictnessStats {
	result := &TypeStrictnessStats{
		registry: registry,

		Good: registry.Counter("middleware.strictness.good"),
		Bad:  registry.Counter("middleware.strictness.bad"),
	}
	return result
}
