package size

import "github.com/transferia/transferia/library/go/core/metrics"

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

func DefaultBuckets() metrics.Buckets {
	return metrics.NewBuckets(
		KiB,
		5*KiB,
		10*KiB,
		50*KiB,
		100*KiB,
		500*KiB,
		MiB,
		5*MiB,
		10*MiB,
		50*MiB,
		100*MiB,
		500*MiB,
		GiB,
	)
}
