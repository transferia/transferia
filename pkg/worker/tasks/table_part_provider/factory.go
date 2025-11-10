package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/worker/tasks/table_splitter"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewTPPGetter(
	ctx context.Context,
	lgr log.Logger,
	sourceStorage abstract.Storage,
	transferID string,
	operationID string,
	workerIndex int,
	sharedMemory abstract.SharedMemory,
) AbstractTablePartProviderGetter {
	if _, isAsyncParts := sourceStorage.(abstract.NextArrTableDescriptionGetterBuilder); isAsyncParts {
		lgr.Infof("NewTPPGetter - factory calls NewTPPGetterAsync")
		return NewTPPGetterAsync(ctx, sharedMemory, transferID, operationID, workerIndex)
	} else {
		lgr.Infof("NewTPPGetter - factory calls NewTPPGetterSync")
		return NewTPPGetterSync(sharedMemory, transferID, operationID, workerIndex)
	}
}

func NewTPPSetter(
	ctx context.Context,
	lgr log.Logger,
	sourceStorage abstract.Storage,
	dstModel model.Destination,
	tables []abstract.TableDescription,
	tmpPolicyConfig *model.TmpPolicyConfig,
	operationID string,
	sharedMemory abstract.SharedMemory,
) (AbstractTablePartProviderSetter, error) {
	if _, isAsyncParts := sourceStorage.(abstract.NextArrTableDescriptionGetterBuilder); isAsyncParts {
		lgr.Infof("NewTPPSetter - factory calls NewTPPSetterAsync")
		result := NewTPPSetterAsync(sharedMemory)
		return result, nil
	} else {
		lgr.Infof("NewTPPSetter - factory calls NewTPPSetterSync")
		parts, err := table_splitter.SplitTables(ctx, logger.Log, sourceStorage, dstModel, tables, tmpPolicyConfig, operationID)
		if err != nil {
			return nil, xerrors.Errorf("unable to shard tables for operation '%v': %w", operationID, err)
		}
		abstract.DumpTablePartsToLogs(parts)

		result, err := NewTPPSetterSync(sharedMemory, parts)
		if err != nil {
			return nil, xerrors.Errorf("unable to store operation tables: %w", err)
		}
		return result, nil
	}
}
