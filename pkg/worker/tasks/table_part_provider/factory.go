package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/worker/tasks/table_splitter"
)

func NewSingleWorkerTPPFull(
	ctx context.Context,
	sourceStorage abstract.Storage,
	dstModel model.Destination,
	tables []abstract.TableDescription,
	tmpPolicyConfig *model.TmpPolicyConfig,
	operationID string,
	workerIndex int,
	coordinator coordinator.Coordinator,
) (AbstractTablePartProviderFull, error) {
	if _, isAsyncParts := sourceStorage.(abstract.AsyncOperationPartsStorage); isAsyncParts {
		logger.Log.Warn("will create table_part_provider: SingleWorkerTPPFullAsync")

		result := NewSingleWorkerTPPFullAsync()
		return result, nil
	} else {
		logger.Log.Warn("will create table_part_provider: SingleWorkerTPPFullSync")

		var err error
		parts, err := table_splitter.SplitTables(ctx, logger.Log, sourceStorage, dstModel, tables, tmpPolicyConfig, operationID)
		if err != nil {
			return nil, xerrors.Errorf("unable to shard tables for operation '%v': %w", operationID, err)
		}
		for _, table := range parts {
			table.WorkerIndex = new(int)
			*table.WorkerIndex = workerIndex // Because we have one worker
		}
		DumpTablePartsToLogs(parts)

		if err := coordinator.CreateOperationTablesParts(operationID, parts); err != nil {
			return nil, xerrors.Errorf("unable to store operation tables: %w", err)
		}

		result := NewSingleWorkerTPPFullSync()
		err = result.AppendParts(ctx, parts)
		if err != nil {
			return nil, xerrors.Errorf("unable to store operation tables: %w", err)
		}
		return result, nil
	}
}

type shardStateGetter func(context.Context) (string, error)

func NewMultiWorkerTPPGetter(
	ctx context.Context,
	sourceStorage abstract.Storage,
	transferID string,
	operationID string,
	workerIndex int,
	coordinator coordinator.Coordinator,
	shardStateGetter shardStateGetter,
) AbstractTablePartProviderGetter {
	if _, isAsyncParts := sourceStorage.(abstract.AsyncOperationPartsStorage); isAsyncParts {
		return NewMultiWorkerTPPGetterAsync(ctx, coordinator, shardStateGetter, operationID, transferID, workerIndex)
	} else {
		return NewMultiWorkerTPPGetterSync(coordinator, operationID, workerIndex)
	}
}

func NewTablePartProviderSetter(
	ctx context.Context,
	sourceStorage abstract.Storage,
	dstModel model.Destination,
	tables []abstract.TableDescription,
	tmpPolicyConfig *model.TmpPolicyConfig,
	operationID string,
	workerIndex int, // <----------------------------------------------------------------------------------------------- candidate for removing
	coordinator coordinator.Coordinator,
) (AbstractTablePartProviderSetter, error) {
	if _, isAsyncParts := sourceStorage.(abstract.AsyncOperationPartsStorage); isAsyncParts {
		result := NewMultiWorkerTPPSetterAsync()
		return result, nil
	} else {
		parts, err := table_splitter.SplitTables(ctx, logger.Log, sourceStorage, dstModel, tables, tmpPolicyConfig, operationID)
		if err != nil {
			return nil, xerrors.Errorf("unable to shard tables for operation '%v': %w", operationID, err)
		}
		DumpTablePartsToLogs(parts)

		if err := coordinator.CreateOperationTablesParts(operationID, parts); err != nil {
			return nil, xerrors.Errorf("unable to store operation tables: %w", err)
		}

		result := NewMultiWorkerTPPSetterSync()
		err = result.AppendParts(ctx, parts)
		if err != nil {
			return nil, xerrors.Errorf("unable to store operation tables: %w", err)
		}
		return result, nil
	}
}
