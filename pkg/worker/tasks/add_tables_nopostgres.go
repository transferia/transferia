//go:build disable_postgres_provider

package tasks

import (
	"sort"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func isAllowedSourceType(source model.Source) bool {
	return false
}

func verifyCanAddTables(source model.Source, tables []string, transfer *model.Transfer) error {
	return xerrors.New("Add tables method is obsolete and supported only for pg sources")
}

func applyAddedTablesSchema(transfer *model.Transfer, registry metrics.Registry) error {
	return nil
}

func replaceSourceTables(source model.Source, targetTables []string) (oldTables []string) {
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
}
