package tasks

import (
	"context"
	"sort"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"go.ytsaurus.tech/library/go/core/log"
)

func AddTables(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task model.TransferOperation, tables []string, registry metrics.Registry) error {
	if transfer.IsTransitional() {
		err := TransitionalAddTables(ctx, cp, transfer, task, tables, registry)
		if err != nil {
			return xerrors.Errorf("Unable to transitional add table: %w", err)
		}
		return nil
	}
	if err := StopJob(cp, transfer); err != nil {
		return xerrors.Errorf("stop job: %w", err)
	}

	if !isAllowedSourceType(transfer.Src) {
		return errors.CategorizedErrorf(categories.Source, "Add tables method is obsolete and supported only for pg sources")
	}

	if err := verifyCanAddTables(transfer.Src, tables, &transfer); err != nil {
		return errors.CategorizedErrorf(categories.Source, "Unable to add tables: %v", err)
	}

	oldTables := replaceSourceTables(transfer.Src, tables)
	commonTableSet := make(map[string]bool)
	for _, table := range oldTables {
		commonTableSet[table] = true
	}
	for _, table := range tables {
		commonTableSet[table] = true
	}

	logger.Log.Info(
		"Initial load for tables",
		log.Any("tables", tables),
	)
	if err := applyAddedTablesSchema(&transfer, registry); err != nil {
		return xerrors.Errorf("failed to transfer schema of the added tables: %w", err)
	}
	snapshotLoader := NewSnapshotLoader(cp, task.OperationID, &transfer, registry)
	if err := snapshotLoader.LoadSnapshot(ctx); err != nil {
		return xerrors.Errorf("failed to load data of the added tables (at snapshot): %w", err)
	}
	logger.Log.Info(
		"Load done, store added tables in source endpoint and start transfer",
		log.Any("tables", tables),
		log.Any("id", transfer.ID),
	)

	e, err := cp.GetEndpoint(transfer.ID, true)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "Cannot load source endpoint for updating: %w", err)
	}
	newSrc, _ := e.(model.Source)
	setSourceTables(newSrc, commonTableSet)
	if err := cp.UpdateEndpoint(transfer.ID, newSrc); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "Cannot store source endpoint with added tables: %w", err)
	}

	return StartJob(ctx, cp, transfer, &task)
}

func isAllowedSourceType(source model.Source) bool {
	switch source.(type) {
	case *postgres.PgSource:
		return true
	}
	return false
}

func verifyCanAddTables(source model.Source, tables []string, transfer *model.Transfer) error {
	switch src := source.(type) {
	case *postgres.PgSource:
		if err := postgres.VerifyPostgresTablesNames(tables); err != nil {
			return xerrors.Errorf("Invalid tables names: %w", err)
		}
		oldTables := src.DBTables
		src.DBTables = tables
		err := postgres.VerifyPostgresTables(src, transfer, logger.Log)
		src.DBTables = oldTables
		if err != nil {
			return xerrors.Errorf("Postgres has no desired tables %v on cluster %v (%w)", tables, src.ClusterID, err)
		} else {
			logger.Log.Infof("Postgres with desired tables %v detected %v", tables, src.ClusterID)
			return nil
		}
	default:
		return xerrors.New("Add tables method is obsolete and supported only for pg sources")
	}
}

func applyAddedTablesSchema(transfer *model.Transfer, registry metrics.Registry) error {
	switch src := transfer.Src.(type) {
	case *postgres.PgSource:
		if src.PreSteps.AnyStepIsTrue() {
			pgdump, err := postgres.ExtractPgDumpSchema(transfer)
			if err != nil {
				return errors.CategorizedErrorf(categories.Source, "failed to extract schema from source: %w", err)
			}
			if err := postgres.ApplyPgDumpPreSteps(pgdump, transfer, registry); err != nil {
				return errors.CategorizedErrorf(categories.Target, "failed to apply pre-steps to transfer schema: %w", err)
			}
		}
		return nil
	}
	return nil
}

func replaceSourceTables(source model.Source, targetTables []string) (oldTables []string) {
	switch src := source.(type) {
	case *postgres.PgSource:
		oldTables = src.DBTables
		src.DBTables = targetTables
	}
	return oldTables
}

func setSourceTables(source model.Source, tableSet map[string]bool) {
	result := make([]string, 0)
	for name, add := range tableSet {
		if add {
			result = append(result, name)
		}
	}
	sort.Strings(result)
	switch src := source.(type) {
	case *postgres.PgSource:
		src.DBTables = result
	}
}
