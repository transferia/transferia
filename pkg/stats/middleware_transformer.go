package stats

import (
	"time"

	"github.com/transferria/transferria/library/go/core/metrics"
)

type MiddlewareTransformerStats struct {
	registry metrics.Registry

	Dropped metrics.Counter
	Errors  metrics.Counter
	Elapsed metrics.Timer
}

func NewMiddlewareTransformerStats(r metrics.Registry) *MiddlewareTransformerStats {
	rWT := r.WithTags(map[string]string{"component": "middleware_transformer"})
	return &MiddlewareTransformerStats{
		registry: rWT,

		Dropped: rWT.Counter("middleware.transformer.dropped"),
		Errors:  rWT.Counter("middleware.transformer.errors"),
		Elapsed: rWT.DurationHistogram("middleware.transformer.elapsed", MillisecondDurationBuckets()),
	}
}

// MillisecondDurationBuckets returns buckets adapted for durations between 1 millisecond and 1 second
func MillisecondDurationBuckets() metrics.DurationBuckets {
	return metrics.NewDurationBuckets(
		500*time.Microsecond,
		1*time.Millisecond,
		2*time.Millisecond,
		3*time.Millisecond,
		4*time.Millisecond,
		5*time.Millisecond,
		10*time.Millisecond,
		20*time.Millisecond,
		30*time.Millisecond,
		40*time.Millisecond,
		50*time.Millisecond,
		60*time.Millisecond,
		70*time.Millisecond,
		80*time.Millisecond,
		90*time.Millisecond,
		100*time.Millisecond,
		200*time.Millisecond,
		400*time.Millisecond,
		600*time.Millisecond,
		800*time.Millisecond,
		1*time.Second,
	)
}
