package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

type SnapshotTableProgressTracker struct {
	ctx             context.Context
	cancel          context.CancelFunc
	pushTicker      *time.Ticker
	waitForComplete sync.WaitGroup
	closed          bool

	operationID         string
	cpClient            coordinator.Coordinator
	parts               map[string]*model.OperationTablePart
	progressUpdateMutex *sync.Mutex

	// TODO: Remove, A2 thing
	progressFuncs map[string]func()
}

func NewSnapshotTableProgressTracker(
	ctx context.Context,
	operationID string,
	cpClient coordinator.Coordinator,
	progressUpdateMutex *sync.Mutex,
) *SnapshotTableProgressTracker {
	ctx, cancel := context.WithCancel(ctx)
	tracker := &SnapshotTableProgressTracker{
		ctx:             ctx,
		cancel:          cancel,
		pushTicker:      nil,
		waitForComplete: sync.WaitGroup{},
		closed:          false,

		operationID:         operationID,
		cpClient:            cpClient,
		parts:               map[string]*model.OperationTablePart{},
		progressUpdateMutex: progressUpdateMutex,

		// TODO: Remove, A2 thing
		progressFuncs: map[string]func(){},
	}

	tracker.waitForComplete.Add(1)
	tracker.pushTicker = time.NewTicker(time.Second * 15)
	go tracker.run()

	return tracker
}

func (t *SnapshotTableProgressTracker) run() {
	defer t.waitForComplete.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.pushTicker.C:
			t.pushProgress()
		}
	}
}

// Close
// Safe to close few time, not thread safe;
// But safe to use with defer and standalone call in same time;
func (t *SnapshotTableProgressTracker) Close() {
	if t.closed {
		return
	}

	t.pushTicker.Stop()
	t.cancel()
	t.waitForComplete.Wait()

	t.pushProgress()
	t.closed = true
}

func (t *SnapshotTableProgressTracker) pushProgress() {
	t.progressUpdateMutex.Lock()

	// TODO: Remove, A2 thing
	for _, progressFunc := range t.progressFuncs {
		progressFunc()
	}

	partsCopy := make([]*model.OperationTablePart, 0, len(t.parts))
	for _, table := range t.parts {
		partsCopy = append(partsCopy, table.Copy())
	}
	t.progressUpdateMutex.Unlock()

	if len(partsCopy) <= 0 {
		return
	}

	if err := t.cpClient.UpdateOperationTablesParts(t.operationID, partsCopy); err != nil {
		logger.Log.Warn(
			fmt.Sprintf("Failed to send tables progress for operation '%v'", t.operationID),
			log.String("OperationID", t.operationID), log.Error(err))
		return // Try next time
	}

	// Clear completed tables parts
	t.progressUpdateMutex.Lock()
	for _, pushedPart := range partsCopy {
		if !pushedPart.Completed {
			continue
		}

		key := pushedPart.Key()
		table, ok := t.parts[key]
		if ok && table.Completed {
			delete(t.parts, key)
		}
	}
	t.progressUpdateMutex.Unlock()
}

func (t *SnapshotTableProgressTracker) Add(part *model.OperationTablePart) {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	t.parts[part.Key()] = part
}

func (t *SnapshotTableProgressTracker) Flush() {
	t.pushProgress()
}

// AddGetProgress TODO: Remove, A2 thing
func (t *SnapshotTableProgressTracker) AddGetProgress(part *model.OperationTablePart, progressFunc func()) {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	t.progressFuncs[part.Key()] = progressFunc
}

// RemoveGetProgress TODO: Remove, A2 thing
func (t *SnapshotTableProgressTracker) RemoveGetProgress(part *model.OperationTablePart) {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	delete(t.progressFuncs, part.Key())
}
