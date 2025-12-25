package table_part_provider

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
)

const assignPartTimeout = 30 * time.Second

// To verify providers contract implementation
var (
	_ AbstractTablePartProviderGetter = (*TPPGetterAsync)(nil)
)

type TPPGetterAsync struct {
	sharedMemory abstract.SharedMemory
	transferID   string
	operationID  string
	workerIndex  int

	pollingErr    error
	isPollingDone atomic.Bool
}

func (g *TPPGetterAsync) SharedMemory() abstract.SharedMemory {
	return g.sharedMemory
}

// statePolling checks operation state, async provider specific function.
func (g *TPPGetterAsync) statePolling(ctx context.Context) error {
	ticker := time.NewTicker(assignPartTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Async PartsCh poller context is cancelled")
			return nil
		case <-ticker.C:
			stateStr, err := g.sharedMemory.GetShardStateNoWait(ctx, g.operationID)
			if err != nil {
				return xerrors.Errorf("unable to get shard state: %w", err)
			}
			if stateStr == "" {
				continue
			}
			var state map[string]any
			if err := jsonx.Unmarshal([]byte(stateStr), &state); err != nil {
				return xerrors.Errorf("unable to unmarshal sharded state as JSON: %w", err)
			}
			isFinished, ok := state[abstract.IsAsyncPartsUploadedStateKey].(bool)
			if !ok {
				return xerrors.Errorf("got unexpected state '%s'", stateStr)
			}
			if isFinished {
				logger.Log.Info("Async PartsCh uploading by source is finished")
				return nil
			}
		}
	}
}

func (g *TPPGetterAsync) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	// Part provider is async and need to wait for more PartsCh.
	for ctx.Err() == nil {
		part, err := g.sharedMemory.NextOperationTablePart(ctx)
		if err != nil {
			return nil, xerrors.Errorf("unable to assign operation table part: %w", err)
		}

		if part != nil {
			return part, nil
		}

		if g.isPollingDone.Load() {
			logger.Log.Info("Async TPPGetterAsync table_part_provider polling is done", log.Error(err))
			if err := g.pollingErr; err != nil {
				return nil, xerrors.Errorf("state polling finished with error: %w", err)
			}
			return nil, nil
		}
		time.Sleep(assignPartTimeout)
	}

	if ctx.Err() != nil {
		logger.Log.Error("TPPGetterAsync table_part_provider cancelled", log.Error(ctx.Err()))
		return nil, xerrors.Errorf("TPPGetterAsync table_part_provider cancelled: %w", ctx.Err())
	}
	return nil, nil
}

func NewTPPGetterAsync(
	ctx context.Context,
	sharedMemory abstract.SharedMemory,
	transferID string,
	operationID string,
	workerIndex int,
) AbstractTablePartProviderGetter {
	result := &TPPGetterAsync{
		sharedMemory:  sharedMemory,
		transferID:    transferID,
		operationID:   operationID,
		workerIndex:   workerIndex,
		pollingErr:    nil,
		isPollingDone: atomic.Bool{},
	}
	go func() {
		defer result.isPollingDone.Store(true)
		result.pollingErr = result.statePolling(ctx)
	}()
	return result
}
