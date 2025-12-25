package tasks

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func CheckRemoveTablesSupported(transfer model.Transfer) error {
	if transfer.IsTransitional() {
		if transfer.AsyncOperations {
			return xerrors.New("RemoveTables is supported only for non-transitional transfers")
		}
		return nil //cannot check more deep from cpl
	}

	if !isAllowedSourceType(transfer.Src) {
		return xerrors.New("RemoveTables is supported only for pg sources")
	}

	return nil
}

func RemoveTables(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task model.TransferOperation, tables []string) error {
	if err := CheckRemoveTablesSupported(transfer); err != nil {
		return xerrors.Errorf("RemoveTables unsupported: %w", err)
	}
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
		switch src := src.(type) {
		case *postgres.PgSource:
			tableSet := make(map[string]bool)
			for _, table := range src.DBTables {
				tableSet[table] = true
			}
			for _, table := range tables {
				tableSet[table] = false
			}
			src.DBTables = make([]string, 0)
			for k, v := range tableSet {
				if v {
					src.DBTables = append(src.DBTables, k)
				}
			}
			c, err := cp.GetEndpoint(transfer.ID, true)
			if err != nil {
				return xerrors.Errorf("Cannot load source endpoint to update tables list changes: %w", err)
			}
			source, _ := c.(model.Source)
			updatedSrc, _ := source.(*postgres.PgSource)
			updatedSrc.DBTables = src.DBTables
			updatedSrc.ExcludedTables = src.ExcludedTables
			if err := cp.UpdateEndpoint(transfer.ID, c); err != nil {
				return xerrors.Errorf("Cannot store source endpoint with tables changes: %w", err)
			}
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
