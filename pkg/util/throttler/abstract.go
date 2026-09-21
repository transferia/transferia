package throttler

import (
	"go.ytsaurus.tech/library/go/core/log"
)

type Throttler interface {
	ExceededLimits() bool
	AddInflight(size uint64)
	ReduceInflight(size uint64)
	InflightBytes() uint64

	WaitLimits(stopCh <-chan struct{}, parseQDone <-chan struct{}, logger log.Logger)
}
