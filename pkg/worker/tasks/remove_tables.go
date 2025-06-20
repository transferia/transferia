package tasks

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func RemoveTables(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task model.TransferOperation, tables []string) error {
	active, err := GetLeftTerminalSrcEndpoints(cp, transfer)
	if err != nil {
		return nil
	}
	if len(active) == 0 {
		return xerrors.New("RemoveTable supports maximum one-lb-in-the-middle case")
	}
	isRunning := transfer.Status == model.Running
	if isRunning {
		if err := StopJob(cp, transfer); err != nil {
			return xerrors.Errorf("stop job: %w", err)
		}
	}
	for _, src := range active {
		if err := removeTableHandleSrc(cp, transfer, src, tables); err != nil {
			return err
		}
	}
	if !isRunning {
		return nil
	}
	if err := StartJob(ctx, cp, transfer, &task); err != nil {
		return xerrors.Errorf("unable to start job: %w", err)
	}

	return nil
}
