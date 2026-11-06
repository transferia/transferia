package progress_tracker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/backoff"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
)

type partsUpdater interface {
	UpdateOperationTablesParts(string, []*abstract.OperationTablePart) error
}

// TableTracker periodically persists snapshot table progress.
type TableTracker struct {
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce *sync.Once

	logFields           []log.Field
	updater             partsUpdater
	operationID         string
	parts               map[string]*abstract.OperationTablePart
	progressUpdateMutex *sync.Mutex
}

func NewTableTracker(
	updater partsUpdater,
	operationID string,
	progressUpdateMutex *sync.Mutex,
	logFields []log.Field,
) *TableTracker {
	ctx, cancel := context.WithCancel(ctxlog.WithFields(context.Background(), logFields...))
	tracker := &TableTracker{
		cancel:    cancel,
		wg:        sync.WaitGroup{},
		closeOnce: &sync.Once{},

		logFields:           logFields,
		updater:             updater,
		operationID:         operationID,
		parts:               map[string]*abstract.OperationTablePart{},
		progressUpdateMutex: progressUpdateMutex,
	}
	tracker.wg.Add(1)
	go tracker.run(ctx)
	return tracker
}

func (t *TableTracker) run(ctx context.Context) {
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
func (t *TableTracker) Close() {
	t.closeOnce.Do(func() {
		t.cancel()
		t.wg.Wait()
		_ = t.Flush(true)
	})
}

func (t *TableTracker) Flush(isTableDone bool) error {
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
		return t.updater.UpdateOperationTablesParts(t.operationID, partsCopy)
	}, currBackOff, backoffutil.Log(ctxlog.WithFields(context.Background(), t.logFields...), "UpdateOperationTablesParts"))
	if err != nil {
		if !isTableDone {
			logger.Warn(
				ctxlog.WithFields(context.Background(), t.logFields...),
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

func (t *TableTracker) Add(part *abstract.OperationTablePart) {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	t.parts[part.Key()] = part
}

func (t *TableTracker) Start(part *abstract.OperationTablePart) {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	part.CompletedRows = 0
	part.Completed = false
	t.parts[part.Key()] = part
}

func (t *TableTracker) IncCompletedRows(part *abstract.OperationTablePart, rows uint64) {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	part.CompletedRows += rows
}

func (t *TableTracker) Complete(part *abstract.OperationTablePart) {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	part.Completed = true
}
