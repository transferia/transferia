package tasks

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

// fast check whether cleanup may be skipped
// if returns false, it can be skipped definitely
// if returns true, we should run cleanup just in case  - this keeps old logic
func CleanupNeeded(transfer model.Transfer) bool {
	if _, ok := providers.DestinationAs[providers.DstCleanuper](&transfer); ok {
		return true
	}

	cleanuper, ok := providers.SourceAs[providers.SrcCleanuper](&transfer)
	return ok && cleanuper.CleanupSuitable(transfer.Type)
}

func CleanupResource(ctx context.Context, task model.TransferOperation, transfer model.Transfer, logger log.Logger, cp coordinator.Coordinator) error {
	if !CleanupNeeded(transfer) {
		return nil
	}

	srcCleanuper, ok := providers.Source[providers.SrcCleanuper](logger, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, &transfer)
	if ok && srcCleanuper.CleanupSuitable(transfer.Type) {
		if err := srcCleanuper.CleanupSource(ctx); err != nil {
			return xerrors.Errorf("unable to cleanup source: %w", err)
		}
	}

	dstCleanuper, ok := providers.Destination[providers.DstCleanuper](logger, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, &transfer, nil)
	if ok {
		if err := dstCleanuper.CleanupDestination(ctx); err != nil {
			return xerrors.Errorf("unable to cleanup destination: %w", err)
		}
	}
	return nil
}
