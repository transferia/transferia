package tasks

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/config/env"
)

var ErrNoActiveOperation = xerrors.NewSentinel("TM: missed operation id")

func StopJob(cp coordinator.Coordinator, transfer model.Transfer) error {
	if transfer.SnapshotOnly() {
		return nil
	}

	if transfer.AsyncOperations {
		return nil
	}

	if err := stopRuntime(cp, transfer); err != nil {
		return xerrors.Errorf("unable to stop runtime hook: %w", err)
	}
	return nil
}

var stopRuntime = func(cp coordinator.Coordinator, transfer model.Transfer) error {
	if env.IsTest() {
		return nil
	}
	return nil
}
