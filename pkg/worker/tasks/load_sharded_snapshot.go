package tasks

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/worker/tasks/table_part_provider"
	"github.com/transferia/transferia/pkg/worker/tasks/table_part_provider/shared_memory"
	"go.ytsaurus.tech/library/go/core/log"
)

func newMetaCheckBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(metaCheckInterval), metaCheckMaxRetries)
}

var (
	metaCheckInterval   time.Duration
	metaCheckMaxRetries uint64
)

func init() {
	metaCheckInterval = 15 * time.Second
	metaCheckMaxRetries = uint64((6 * time.Hour) / metaCheckInterval)
}

func (l *SnapshotLoader) WaitWorkersInitiated(ctx context.Context) error {
	return backoff.RetryNotify(
		func() error {
			workersCount, err := l.cp.GetOperationWorkersCount(l.operationID, false)
			if err != nil {
				return errors.CategorizedErrorf(categories.Internal, "can't to get workers count for operation '%v': %w", l.operationID, err)
			}
			if workersCount <= 0 {
				return errors.CategorizedErrorf(categories.Internal, "workers for operation '%v' not ready yet", l.operationID)
			}
			return nil
		},
		backoff.WithContext(newMetaCheckBackoff(), ctx),
		util.BackoffLoggerDebug(logger.Log, "waiting for creating operation workers rows"),
	)
}

func (l *SnapshotLoader) ReadFromCPShardState(ctx context.Context) (string, error) {
	if err := l.WaitWorkersInitiated(ctx); err != nil {
		return "", errors.CategorizedErrorf(categories.Internal, "failed while waiting for sharded task metadata initialization: %w", err)
	}
	return shared_memory.GetShardStateNoWait(ctx, l.cp, l.operationID)
}

// OperationStateExists returns true if the state of the operation of the given task exists (is not nil)
func (l *SnapshotLoader) OperationStateExists(ctx context.Context) (bool, error) {
	result, err := backoff.RetryNotifyWithData(
		func() (bool, error) {
			_, err := l.cp.GetOperationState(l.operationID)
			if err != nil {
				if xerrors.Is(err, coordinator.OperationStateNotFoundError) {
					return false, nil
				}
				return false, xerrors.Errorf("failed to get operation state: %w", err)
			}
			return true, nil
		},
		backoff.WithContext(newMetaCheckBackoff(), ctx),
		util.BackoffLoggerDebug(logger.Log, "waiting for sharded state"),
	)
	return result, err
}

func (l *SnapshotLoader) MainWorkerCreateShardedStateFromSource(source interface{}) (string, error) {
	if shardingContextStorage, ok := source.(abstract.ShardingContextStorage); ok {
		shardCtx, err := shardingContextStorage.ShardingContext()
		if err != nil {
			return "", errors.CategorizedErrorf(categories.Internal, "can't get sharded state from source: %w", err)
		}
		return string(shardCtx), nil
	}
	return "", nil
}

func (l *SnapshotLoader) enrichShardedState(storage abstract.Storage, tablePartProviderSetter table_part_provider.AbstractTablePartProviderSetter) error {
	if shardingContextStorage, ok := storage.(abstract.ShardingContextStorage); ok {
		shardedStateBytes, err := shardingContextStorage.ShardingContext()
		if err != nil {
			return xerrors.Errorf("unable to prepare sharded state for operation '%v': %w", l.operationID, err)
		}
		shardedState := string(shardedStateBytes)
		shardedState, err = tablePartProviderSetter.EnrichShardedState(shardedState)
		if err != nil {
			return xerrors.Errorf("unable to enrich sharded state: %w", err)
		}

		logger.Log.Info("will upload sharded state", log.Any("state", shardedState))
		err = l.cp.SetOperationState(l.operationID, shardedState)
		if err != nil {
			return xerrors.Errorf("unable to set sharded state: %w", err)
		}
	}
	return nil
}

func (l *SnapshotLoader) SetShardedStateToSource(source interface{}, shardedState string) error {
	if shardingContextStorage, ok := source.(abstract.ShardingContextStorage); ok && shardedState != "" {
		if err := shardingContextStorage.SetShardingContext([]byte(shardedState)); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "can't set sharded state to source: %w", err)
		}
	}
	return nil
}

func (l *SnapshotLoader) WaitWorkersCompleted(ctx context.Context, sourceStorage abstract.Storage, workersCount int) error {
	startTime := time.Now()
	if err := l.WaitWorkersInitiated(ctx); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to wait workers initiated: %w", err)
	}
	for {
		if customCheck, ok := sourceStorage.(abstract.CustomCheckSecondaryWorkersDone); !ok {
			isDone, err := defaultCheckAreWorkersDone(startTime, l.cp, l.operationID, workersCount)
			if err != nil {
				return errors.CategorizedErrorf(categories.Internal, "an error occured during default waiting if secondary workers completed, err: %w", err)
			}
			if isDone {
				return nil
			}
		} else {
			isDone, err := customCheck.CheckSecondaryWorkersDone(startTime, l.cp, l.transfer)
			if err != nil {
				return xerrors.Errorf("an error occured during custom waiting if secondary workers completed: %w", err)
			}
			if isDone {
				return nil
			}
		}
		time.Sleep(metaCheckInterval)
	}
}
