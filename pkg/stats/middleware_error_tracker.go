package stats

import "github.com/transferia/transferia/library/go/core/metrics"

type MiddlewareErrorTrackerStats struct {
	Failures  metrics.Counter
	Successes metrics.Counter
}

func NewMiddlewareErrorTrackerStats(r metrics.Registry) *MiddlewareErrorTrackerStats {
	rWT := r.WithTags(map[string]string{"component": "middleware_error_tracker"})
	return &MiddlewareErrorTrackerStats{
		Failures:  rWT.Counter("middleware.error_tracker.failures"),
		Successes: rWT.Counter("middleware.error_tracker.success"),
	}
}
