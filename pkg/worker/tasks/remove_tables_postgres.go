//go:build !disable_postgres_provider

package tasks

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func removeTableHandleSrc(cp coordinator.Coordinator, transfer model.Transfer, src model.Source, tables []string) error {
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

	return nil
}
