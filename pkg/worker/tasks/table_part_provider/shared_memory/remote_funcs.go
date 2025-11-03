package shared_memory

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
)

func NewMetaCheckBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(MetaCheckInterval), MetaCheckMaxRetries)
}

const (
	MetaCheckInterval   time.Duration = 15 * time.Second
	MetaCheckMaxRetries uint64        = uint64((6 * time.Hour) / MetaCheckInterval)
)

// GetShardStateNoWait is GetShardState, but do not calls WaitWorkersInitiated.
func GetShardStateNoWait(
	ctx context.Context,
	cp coordinator.Coordinator,
	operationID string,
) (string, error) {
	result, err := backoff.RetryNotifyWithData(
		func() (string, error) {
			stateMsg, err := cp.GetOperationState(operationID)
			if err != nil {
				if xerrors.Is(err, coordinator.OperationStateNotFoundError) {
					return "", nil
				}
				return "", errors.CategorizedErrorf(categories.Internal, "failed to get operation state: %w", err)
			}
			return stateMsg, nil
		},
		backoff.WithContext(NewMetaCheckBackoff(), ctx),
		util.BackoffLoggerDebug(logger.Log, "waiting for sharded state"),
	)
	if err != nil {
		return "", errors.CategorizedErrorf(categories.Internal, "failed while waiting for sharded task state: %w", err)
	}
	return result, nil
}

func ConvertArrTablePartsToTD(
	sharedMemory abstract.SharedMemory,
	parts []*abstract.OperationTablePart,
) ([]abstract.TableDescription, error) {
	tdArr := make([]abstract.TableDescription, 0, len(parts))
	for _, part := range parts {
		td, err := sharedMemory.ConvertToTableDescription(part)
		if err != nil {
			return nil, xerrors.Errorf("failed to convert to table description: %w", err)
		}
		tdArr = append(tdArr, *td)
	}
	return tdArr, nil
}
