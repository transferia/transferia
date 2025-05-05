package tasks

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/cleanup"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

// fast check whether cleanup may be skipped
// if returns false, it can be skipped definitely
// if returns true, we should run cleanup just in case  - this keeps old logic
func CleanupNeeded(transfer model.Transfer) bool {
	if _, ok := transfer.Dst.(model.TmpPolicyProvider); ok && transfer.TmpPolicy != nil {
		return true
	}

	if transfer.SnapshotOnly() {
		return false
	}

	return providers.SourceIs[providers.Cleanuper](&transfer)
}

func CleanupResource(ctx context.Context, task model.TransferOperation, transfer model.Transfer, logger log.Logger, cp coordinator.Coordinator) error {
	if !CleanupNeeded(transfer) {
		return nil
	}

	err := cleanupTmp(ctx, transfer, logger, cp, task)
	if err != nil {
		return xerrors.Errorf("unable to cleanup tmp: %w", err)
	}

	if transfer.SnapshotOnly() {
		return nil
	}

	cleanuper, ok := providers.Source[providers.Cleanuper](logger, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, &transfer)
	if !ok {
		logger.Infof("CleanupResource(%v) for transfer(%v) has no active resource", task.OperationID, transfer.ID)
		return nil
	}
	return cleanuper.Cleanup(ctx, &task)
}

func cleanupTmp(ctx context.Context, transfer model.Transfer, logger log.Logger, cp coordinator.Coordinator, task model.TransferOperation) error {
	tmpPolicy := transfer.TmpPolicy
	if tmpPolicy == nil {
		logger.Info("tmp policy is not set")
		return nil
	}

	err := model.EnsureTmpPolicySupported(transfer.Dst, &transfer)
	if err != nil {
		return xerrors.Errorf(model.ErrInvalidTmpPolicy, err)
	}

	cleanuper, ok := providers.Destination[providers.TMPCleaner](logger, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, &transfer)
	if !ok {
		return nil
	}

	tmpCleaner, err := cleanuper.TMPCleaner(ctx, &task)
	if err != nil {
		return xerrors.Errorf("unable to initialize tmp cleaner: %w", err)
	}
	defer cleanup.Close(tmpCleaner, logger)

	err = tmpCleaner.CleanupTmp(ctx, transfer.ID, tmpPolicy)
	if err == nil {
		logger.Info("successfully cleaned up tmp")
	}
	return err
}
