package tasks

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
)

const assignPartTimeout = 30 * time.Second

type remoteTablePartProvider struct {
	cp          coordinator.Coordinator
	operationID string
	workerIndex int

	// Async-specific fields below.
	isAsync       bool
	transferID    string
	pollingErr    error
	isPollingDone atomic.Bool
}

type ShardStateGetter func(context.Context) (string, error)

// statePolling checks operation state, async provider specific function.
func statePolling(ctx context.Context, getShardState ShardStateGetter) error {
	ticker := time.NewTicker(assignPartTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Async parts poller context is cancelled")
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
				logger.Log.Info("Async parts uploading by source is finished")
				return nil
			}
		}
	}
}

func (p *remoteTablePartProvider) nextTablePart(ctx context.Context) (*model.OperationTablePart, error) {
	if !p.isAsync {
		return p.cp.AssignOperationTablePart(p.operationID, p.workerIndex)
	}

	// Part provider is async and need to wait for more parts.
	for ctx.Err() == nil {
		part, err := p.cp.AssignOperationTablePart(p.operationID, p.workerIndex)
		if err != nil {
			return nil, xerrors.Errorf("unable to assign operation table part: %w", err)
		}

		if part != nil {
			return part, nil
		}

		if p.isPollingDone.Load() {
			logger.Log.Info("Async remote part provider polling is done", log.Error(err))
			if err := p.pollingErr; err != nil {
				return nil, xerrors.Errorf("state polling finished with error: %w", err)
			}
			return nil, nil
		}
		time.Sleep(assignPartTimeout)
	}

	if ctx.Err() != nil {
		logger.Log.Error("Remote part provider cancelled", log.Error(ctx.Err()))
		return nil, xerrors.Errorf("remote part provider cancelled: %w", ctx.Err())
	}
	return nil, nil
}

func (p *remoteTablePartProvider) TablePartProvider() TablePartProvider {
	return p.nextTablePart
}

func NewRemoteTablePartProvider(
	_ context.Context, cp coordinator.Coordinator, operationID string, workerIndex int,
) TablePartProvider {
	provider := &remoteTablePartProvider{
		cp:          cp,
		operationID: operationID,
		workerIndex: workerIndex,

		isAsync:       false,
		transferID:    "",
		pollingErr:    nil,
		isPollingDone: atomic.Bool{},
	}
	return provider.nextTablePart
}

func NewAsyncRemoteTablePartProvider(
	ctx context.Context, cp coordinator.Coordinator, getShardState ShardStateGetter, operationID, transferID string, workerIndex int,
) TablePartProvider {
	provider := &remoteTablePartProvider{
		cp:          cp,
		operationID: operationID,
		workerIndex: workerIndex,

		isAsync:       true,
		transferID:    transferID,
		pollingErr:    nil,
		isPollingDone: atomic.Bool{},
	}
	go func() {
		defer provider.isPollingDone.Store(true)
		provider.pollingErr = statePolling(ctx, getShardState)
	}()
	return provider.nextTablePart
}
