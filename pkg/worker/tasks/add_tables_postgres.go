//go:build !disable_postgres_provider

package tasks

import (
	"sort"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

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
