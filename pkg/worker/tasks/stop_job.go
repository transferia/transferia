package tasks

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/config/env"
)

var ErrNoActiveOperation = xerrors.NewSentinel("TM: missed operation id")

func StopJob(cp coordinator.Coordinator, transfer model.Transfer) error {
	if transfer.SnapshotOnly() {
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
