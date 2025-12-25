package stats

import (
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/util/size"
)

type MiddlewareBuffererStats struct {
	registry metrics.Registry

	FlushOnAllCauses metrics.Counter
	FlushOnInterval  metrics.Counter
	FlushOnCount     metrics.Counter
	FlushOnSize      metrics.Counter
	FlushOnNonRow    metrics.Counter

	// CollectionTime tracks the time spent on collection of changeitems in the buffer (time between flush attempts).
	//
	// When this time is greater than `WaitTime`, this means transfer reads from the source slower than it writes into the destination. Source slows down the whole transfer.
	//
	// When `WaitTime` is greater than this time, this means transfer writes into the destination slower than it reads from the source. Destination slows down the whole transfer.
	CollectionTime metrics.Timer
	// WaitTime tracks the time spent waiting for another flush to finish (time of doing nothing during a flush attempt).
	WaitTime     metrics.Timer
	SizeToFlush  metrics.Histogram
	CountToFlush metrics.Histogram
}

// ShortEvenDurationBuckets returns buckets adapted for short durations and distributed approximately evenly
func ShortEvenDurationBuckets() metrics.DurationBuckets {
	return metrics.NewDurationBuckets(
		500*time.Millisecond,
		1*time.Second,
		2*time.Second,
		3*time.Second,
		4*time.Second,
		5*time.Second,
		6*time.Second,
		7*time.Second,
		8*time.Second,
		9*time.Second,
		10*time.Second,
		12*time.Second,
		14*time.Second,
		16*time.Second,
		18*time.Second,
		20*time.Second,
		25*time.Second,
		30*time.Second,
		35*time.Second,
		40*time.Second,
		45*time.Second,
		50*time.Second,
		55*time.Second,
		1*time.Minute,
		1*time.Minute+15*time.Second,
		1*time.Minute+30*time.Second,
		1*time.Minute+45*time.Second,
		2*time.Minute,
		2*time.Minute+30*time.Second,
		3*time.Minute,
		3*time.Minute+30*time.Second,
		4*time.Minute,
		4*time.Minute+30*time.Second,
		5*time.Minute,
		6*time.Minute,
		7*time.Minute,
		8*time.Minute,
		9*time.Minute,
		10*time.Minute,
	)
}

// Exponential10Buckets returns a set of buckets with borders at 10^[1..10]
func Exponential10Buckets() metrics.Buckets {
	return metrics.MakeExponentialBuckets(10, 10, 10)
}

func NewMiddlewareBuffererStats(r metrics.Registry) *MiddlewareBuffererStats {
	rWT := r.WithTags(map[string]string{"component": "middleware_bufferer"})
	return &MiddlewareBuffererStats{
		registry: rWT,

		FlushOnAllCauses: rWT.Counter("middleware.bufferer.flush.cause.any"),
		FlushOnInterval:  rWT.Counter("middleware.bufferer.flush.cause.interval"),
		FlushOnCount:     rWT.Counter("middleware.bufferer.flush.cause.count"),
		FlushOnSize:      rWT.Counter("middleware.bufferer.flush.cause.size"),
		FlushOnNonRow:    rWT.Counter("middleware.bufferer.flush.cause.nonrow"),

		CollectionTime: rWT.DurationHistogram("middleware.bufferer.collection_time", ShortEvenDurationBuckets()),
		WaitTime:       rWT.DurationHistogram("middleware.bufferer.wait_time", ShortEvenDurationBuckets()),
		SizeToFlush:    rWT.Histogram("middleware.bufferer.flush.size", size.DefaultBuckets()),
		CountToFlush:   rWT.Histogram("middleware.bufferer.flush.count", Exponential10Buckets()),
	}
}
