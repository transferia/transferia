//go:build !disable_postgres_provider

package tasks

import (
	"context"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers/greenplum"
	"go.ytsaurus.tech/library/go/core/log"
)

func beginSnapshotGreenplum(ctx context.Context, l *SnapshotLoader, specificStorage *greenplum.Storage, tables []abstract.TableDescription) error {
	if err := specificStorage.BeginGPSnapshot(ctx, tables); err != nil {
		return errors.CategorizedErrorf(categories.Source, "failed to initialize a Greenplum snapshot: %w", err)
	}
	if !l.transfer.SnapshotOnly() {
		var err error
		l.slotKiller, l.slotKillerErrorChannel, err = specificStorage.RunSlotMonitor(ctx, l.transfer.Src, l.registry)
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "failed to start liveness monitor for Greenplum storage: %w", err)
		}
	}
	workersGpConfig := specificStorage.WorkersGpConfig()
	logger.Log.Info(
		"Greenplum snapshot source runtime configuration",
		log.Any("cluster", workersGpConfig.GetCluster()),
		log.Array("sharding", workersGpConfig.GetWtsList()),
	)

	return nil
}

func endSnapshotGreenplum(ctx context.Context, specificStorage *greenplum.Storage) error {
	esCtx, esCancel := context.WithTimeout(context.Background(), greenplum.PingTimeout)
	defer esCancel()
	if err := specificStorage.EndGPSnapshot(esCtx); err != nil {
		logger.Log.Error("Failed to end snapshot in Greenplum", log.Error(err))
		// When we are here, snapshot could not be finished on coordinator.
		// This may be due to various reasons, which include transaction failure (e.g. due to coordinator-standby fallback).
		// For this reason, we must retry the transfer, as the data obtained from Greenplum segments may be inconsistent.
		return errors.CategorizedErrorf(categories.Source, "failed to end snapshot in Greenplum (on coordinator): %w", err)
	}
	return nil
}
