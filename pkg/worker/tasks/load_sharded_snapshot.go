package tasks

import (
	"context"
	"sort"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func newMetaCheckBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(metaCheckInterval), metaCheckMaxRetries)
}

const (
	metaCheckInterval   time.Duration = 15 * time.Second
	metaCheckMaxRetries uint64        = uint64((6 * time.Hour) / metaCheckInterval)
)

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

	result, err := backoff.RetryNotifyWithData(
		func() (string, error) {
			stateMsg, err := l.cp.GetOperationState(l.operationID)
			if err != nil {
				if xerrors.Is(err, coordinator.OperationStateNotFoundError) {
					return "", nil
				}
				return "", errors.CategorizedErrorf(categories.Internal, "failed to get operation state: %w", err)
			}
			return stateMsg, nil
		},
		backoff.WithContext(newMetaCheckBackoff(), ctx),
		util.BackoffLoggerDebug(logger.Log, "waiting for sharded state"),
	)
	if err != nil {
		return "", errors.CategorizedErrorf(categories.Internal, "failed while waiting for sharded task state: %w", err)
	}
	return result, nil
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

func (l *SnapshotLoader) SplitTables(
	ctx context.Context,
	logger log.Logger,
	tables []abstract.TableDescription,
	source abstract.Storage,
) ([]*model.OperationTablePart, error) {
	var tablesParts []*model.OperationTablePart
	addTablesParts := func(shardedTable ...abstract.TableDescription) {
		for i, shard := range shardedTable {
			operationTable := model.NewOperationTablePartFromDescription(l.operationID, &shard)
			operationTable.PartsCount = uint64(len(shardedTable))
			operationTable.PartIndex = uint64(i)
			tablesParts = append(tablesParts, operationTable)
		}
	}

	shardingStorage, isShardingStorage := source.(abstract.ShardingStorage)
	isShardeableDestination := model.IsShardeableDestination(l.transfer.Dst)
	isTmpPolicyEnabled := l.transfer.TmpPolicy != nil

	reasonWhyNotSharded := ""
	if !isShardingStorage && !isShardeableDestination {
		reasonWhyNotSharded = "Source storage is not supported sharding table, and destination is not supported shardable snapshots - that's why tables won't be sharded here"
	} else if !isShardingStorage {
		reasonWhyNotSharded = "Source storage is not supported sharding table - that's why tables won't be sharded here"
	} else if !isShardeableDestination {
		reasonWhyNotSharded = "Destination is not supported shardable snapshots - that's why tables won't be sharded here"
	} else if isTmpPolicyEnabled {
		reasonWhyNotSharded = "Sharding is not supported by tmp policy, disabling it"
	}

	for _, table := range tables {
		if reasonWhyNotSharded == "" {
			tableParts, err := shardingStorage.ShardTable(ctx, table)
			if err != nil {
				if abstract.IsNonShardableError(err) {
					logger.Info("Unable to shard table", log.String("table", table.Fqtn()), log.Error(err))
					addTablesParts([]abstract.TableDescription{table}...)
				} else {
					return nil, xerrors.Errorf("unable to split table, err: %w", err)
				}
			}
			addTablesParts(tableParts...)
		} else {
			logger.Info(
				"table is not sharded (as all another tables)",
				log.String("table", table.Fqtn()),
				log.String("reason", reasonWhyNotSharded),
			)
			addTablesParts(table)
		}
	}

	// Big tables(or tables parts) go first;
	// This sort is same with sort on select from database;
	sort.Slice(tablesParts, func(i int, j int) bool {
		return util.Less(
			util.NewComparator(-tablesParts[i].ETARows, -tablesParts[j].ETARows),   // Sort desc
			util.NewComparator(tablesParts[i].Schema, tablesParts[j].Schema),       // Sort asc
			util.NewComparator(tablesParts[i].Name, tablesParts[j].Name),           // Sort asc
			util.NewComparator(tablesParts[i].PartIndex, tablesParts[j].PartIndex), // Sort asc
		)
	})

	return tablesParts, nil
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

func (l *SnapshotLoader) SetShardedStateToSource(source interface{}, shardedState string) error {
	if shardingContextStorage, ok := source.(abstract.ShardingContextStorage); ok && shardedState != "" {
		if err := shardingContextStorage.SetShardingContext([]byte(shardedState)); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "can't set sharded state to source: %w", err)
		}
	}
	return nil
}

func (l *SnapshotLoader) SetShardedStateToCP(logger log.Logger, shardedState string) error {
	if err := l.cp.SetOperationState(l.operationID, shardedState); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to store upload shards: %w", err)
	}
	logger.Info("sharded state uploaded", log.Any("state", string(shardedState)))
	return nil
}

func (l *SnapshotLoader) WaitWorkersCompleted(ctx context.Context, workersCount int) error {
	start := time.Now()
	if err := l.WaitWorkersInitiated(ctx); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to wait workers initiated: %w", err)
	}
	for {
		totalProgress, err := l.cp.GetOperationProgress(l.operationID)
		if err != nil {
			return errors.CategorizedErrorf(categories.Internal, "can't to get progress for operation '%v': %w", l.operationID, err)
		}
		workers, err := l.cp.GetOperationWorkers(l.operationID)
		if err != nil {
			return errors.CategorizedErrorf(categories.Internal, "can't to get workers for operation '%v': %w", l.operationID, err)
		}

		if workersCount != len(workers) {
			return errors.CategorizedErrorf(categories.Internal, "expected workers count '%v' not equal real workers count '%v' for operation '%v'",
				workersCount, len(workers), l.operationID)
		}

		completedWorkersCount := 0
		partsInProgress := int64(0)
		var errs util.Errors
		progress := model.NewAggregatedProgress()
		progress.PartsCount = totalProgress.PartsCount
		progress.ETARowsCount = totalProgress.ETARowsCount
		for _, worker := range workers {
			if worker.Completed {
				completedWorkersCount++
			}

			if worker.Err != "" {
				errs = append(errs, xerrors.Errorf("secondary worker [%v] of operation '%v' failed: %v",
					worker.WorkerIndex, l.operationID, worker.Err))
			}

			if worker.Progress != nil {
				partsInProgress += worker.Progress.PartsCount - worker.Progress.CompletedPartsCount
				progress.CompletedPartsCount += worker.Progress.CompletedPartsCount
				progress.CompletedRowsCount += worker.Progress.CompletedRowsCount
			}
		}

		completedWorkersCountPercent := float64(0)
		if workersCount != 0 {
			completedWorkersCountPercent = (float64(completedWorkersCount) / float64(workersCount)) * 100
		}

		completed := (int(completedWorkersCount) == workersCount)

		status := "running"
		if completed {
			status = "completed"
		}
		logger.Log.Infof(
			"Secondary workers are %v, workers: %v in progress, %v completed, %v total (%.2f%% completed), parts: %v in progress, %v completed, %v total (%.2f%% completed), rows: %v / %v (%.2f%%), elapsed time: %v",
			status,
			workersCount-completedWorkersCount, completedWorkersCount, workersCount, completedWorkersCountPercent,
			partsInProgress, progress.CompletedPartsCount, progress.PartsCount, progress.PartsPercent(),
			progress.CompletedRowsCount, progress.ETARowsCount, progress.RowsPercent(),
			time.Since(start))

		if len(errs) > 0 {
			return xerrors.Errorf("errors detected on secondary workers: %v", errs)
		}

		if completed {
			return nil
		}

		time.Sleep(metaCheckInterval)
	}
}
