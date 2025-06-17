//go:build !disable_postgres_provider

package tasks

import (
	"context"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"go.ytsaurus.tech/library/go/core/log"
)

func beginSnapshotPg(ctx context.Context, l *SnapshotLoader, specificStorage *postgres.Storage) error {
	err := specificStorage.BeginPGSnapshot(ctx)
	if err != nil {
		// TODO: change to fatal?
		logger.Log.Warn("unable to begin snapshot", log.Error(err))
	} else {
		logger.Log.Infof("begin postgres snapshot on lsn: %v", specificStorage.SnapshotLSN())
	}
	if !l.transfer.SnapshotOnly() {
		var err error
		tracker := postgres.NewTracker(l.transfer.ID, l.cp)
		l.slotKiller, l.slotKillerErrorChannel, err = specificStorage.RunSlotMonitor(ctx, l.transfer.Src, l.registry, tracker)
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "failed to start slot monitor: %w", err)
		}
	}

	return nil
}

func endSnapshotPg(ctx context.Context, specificStorage *postgres.Storage) {
	if err := specificStorage.EndPGSnapshot(ctx); err != nil {
		logger.Log.Error("Failed to end snapshot in PostgreSQL", log.Error(err))
	}
}
