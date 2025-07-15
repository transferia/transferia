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
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce *sync.Once

	operationID         string
	cpClient            coordinator.Coordinator
	parts               map[string]*model.OperationTablePart
	progressUpdateMutex *sync.Mutex
}

func NewSnapshotTableProgressTracker(
	ctx context.Context, operationID string, cpClient coordinator.Coordinator, progressUpdateMutex *sync.Mutex,
) *SnapshotTableProgressTracker {
	ctx, cancel := context.WithCancel(ctx)
	tracker := &SnapshotTableProgressTracker{
		cancel:    cancel,
		wg:        sync.WaitGroup{},
		closeOnce: &sync.Once{},

		operationID:         operationID,
		cpClient:            cpClient,
		parts:               map[string]*model.OperationTablePart{},
		progressUpdateMutex: progressUpdateMutex,
	}
	tracker.wg.Add(1)
	go tracker.run(ctx)
	return tracker
}

func (t *SnapshotTableProgressTracker) run(ctx context.Context) {
	defer t.wg.Done()
	pushTicker := time.NewTicker(time.Minute)
	defer pushTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pushTicker.C:
			t.Flush()
		}
	}
}

// Close is thread-safe. Only first call will make sense.
func (t *SnapshotTableProgressTracker) Close() {
	t.closeOnce.Do(func() {
		t.cancel()
		t.wg.Wait()
		t.Flush()
	})
}

func (t *SnapshotTableProgressTracker) Flush() {
	t.progressUpdateMutex.Lock()
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
