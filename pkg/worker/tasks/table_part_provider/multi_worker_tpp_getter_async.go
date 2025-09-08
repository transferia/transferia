package table_part_provider

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
)

const assignPartTimeout = 30 * time.Second

type MultiWorkerTPPGetterAsync struct {
	cp            coordinator.Coordinator
	operationID   string
	workerIndex   int
	transferID    string
	pollingErr    error
	isPollingDone atomic.Bool
}

// statePolling checks operation state, async provider specific function.
func statePolling(ctx context.Context, getShardState shardStateGetter) error {
	ticker := time.NewTicker(assignPartTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Async PartsCh poller context is cancelled")
			return nil
		case <-ticker.C:
			stateStr, err := getShardState(ctx)
			if err != nil {
				return xerrors.Errorf("unable to get shard state: %w", err)
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

func (g *MultiWorkerTPPGetterAsync) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	// Part provider is async and need to wait for more PartsCh.
	for ctx.Err() == nil {
		part, err := g.cp.AssignOperationTablePart(g.operationID, g.workerIndex)
		if err != nil {
			return nil, xerrors.Errorf("unable to assign operation table part: %w", err)
		}

		if part != nil {
			return part, nil
		}

		if g.isPollingDone.Load() {
			logger.Log.Info("Async MultiWorkerTPPGetterAsync table_part_provider polling is done", log.Error(err))
			if err := g.pollingErr; err != nil {
				return nil, xerrors.Errorf("state polling finished with error: %w", err)
			}
			return nil, nil
		}
		time.Sleep(assignPartTimeout)
	}

	if ctx.Err() != nil {
		logger.Log.Error("MultiWorkerTPPGetterAsync table_part_provider cancelled", log.Error(ctx.Err()))
		return nil, xerrors.Errorf("MultiWorkerTPPGetterAsync table_part_provider cancelled: %w", ctx.Err())
	}
	return nil, nil
}

func (g *MultiWorkerTPPGetterAsync) AllPartsOrNil() []*abstract.OperationTablePart {
	return nil
}

func NewMultiWorkerTPPGetterAsync(
	ctx context.Context,
	cp coordinator.Coordinator,
	getShardState shardStateGetter,
	operationID string,
	transferID string,
	workerIndex int,
) AbstractTablePartProviderGetter {
	result := &MultiWorkerTPPGetterAsync{
		cp:            cp,
		operationID:   operationID,
		workerIndex:   workerIndex,
		transferID:    transferID,
		pollingErr:    nil,
		isPollingDone: atomic.Bool{},
	}
	go func() {
		defer result.isPollingDone.Store(true)
		result.pollingErr = statePolling(ctx, getShardState)
	}()
	return result
}
