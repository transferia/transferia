package throttler

import "go.ytsaurus.tech/library/go/core/log"

type StubThrottler struct{}

func (t *StubThrottler) ExceededLimits() bool {
	return false
}

func (t *StubThrottler) AddInflight(_ uint64) {}

func (t *StubThrottler) ReduceInflight(_ uint64) {}

func (t *StubThrottler) InflightBytes() uint64 {
	return 0
}

func (t *StubThrottler) WaitLimits(_ <-chan struct{}, _ <-chan struct{}, _ log.Logger) {}

func NewStubThrottler() Throttler {
	return &StubThrottler{}
}
