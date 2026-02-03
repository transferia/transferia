package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type SnapshotTableProgressTracker struct {
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce *sync.Once

	sharedMemory        abstract.SharedMemory
	operationID         string
	parts               map[string]*abstract.OperationTablePart
	progressUpdateMutex *sync.Mutex
}

func NewSnapshotTableProgressTracker(
	ctx context.Context,
	sharedMemory abstract.SharedMemory,
	operationID string,
	progressUpdateMutex *sync.Mutex,
) *SnapshotTableProgressTracker {
	ctx, cancel := context.WithCancel(ctx)
	tracker := &SnapshotTableProgressTracker{
		cancel:    cancel,
		wg:        sync.WaitGroup{},
		closeOnce: &sync.Once{},

		sharedMemory:        sharedMemory,
		operationID:         operationID,
		parts:               map[string]*abstract.OperationTablePart{},
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
			_ = t.Flush(false)
		}
	}
}

// Close is thread-safe. Only first call will make sense.
func (t *SnapshotTableProgressTracker) Close() {
	t.closeOnce.Do(func() {
		t.cancel()
		t.wg.Wait()
		_ = t.Flush(true)
	})
}

func (t *SnapshotTableProgressTracker) Flush(isTableDone bool) error {
	t.progressUpdateMutex.Lock()
	partsCopy := make([]*abstract.OperationTablePart, 0, len(t.parts))
	for _, table := range t.parts {
		partsCopy = append(partsCopy, table.Copy())
	}
	t.progressUpdateMutex.Unlock()

	if len(partsCopy) == 0 {
		return nil
	}

	var currBackOff backoff.BackOff = &backoff.StopBackOff{}
	if isTableDone {
		currBackOff = backoff.NewExponentialBackOff()
	}
	err := backoff.RetryNotify(func() error {
		return t.sharedMemory.UpdateOperationTablesParts(t.operationID, partsCopy)
	}, currBackOff, util.BackoffLoggerWarn(logger.Log, "UpdateOperationTablesParts"))
	if err != nil {
		if !isTableDone {
			logger.Log.Warn(
				fmt.Sprintf("Failed to send tables progress for operation '%v'", t.operationID),
				log.String("OperationID", t.operationID),
				log.Error(err),
			)
			return nil
		}
		return xerrors.Errorf("failed to update operation tables parts, err: %w", err)
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
	return nil
}

func (t *SnapshotTableProgressTracker) Add(part *abstract.OperationTablePart) {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	t.parts[part.Key()] = part
}
