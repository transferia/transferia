package queue_to_s3_sink

import (
	"context"
	"sync"

	"github.com/transferia/transferia/pkg/abstract"
)

type lifecycleState struct {
	// This mutex prevents data corruption when regular rotation and AsyncV2Push work concurrently
	mu      sync.Mutex
	pushCtx context.Context
	resCh   chan<- abstract.AsyncPushResult

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newLifecycleState() *lifecycleState {
	return &lifecycleState{
		mu:       sync.Mutex{},
		pushCtx:  nil, // Latched by the first push
		resCh:    nil,
		stopCh:   make(chan struct{}),
		stopOnce: sync.Once{},
		wg:       sync.WaitGroup{},
	}
}
