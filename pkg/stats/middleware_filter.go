package stats

import "github.com/transferia/transferia/library/go/core/metrics"

type MiddlewareFilterStats struct {
	registry metrics.Registry

	Dropped metrics.Counter
}

func NewMiddlewareFilterStats(r metrics.Registry) *MiddlewareFilterStats {
	rWT := r.WithTags(map[string]string{"component": "middleware_filter"})
	return &MiddlewareFilterStats{
		registry: rWT,

		Dropped: rWT.Counter("middleware.filter.dropped"),
	}
}
