//go:build !disable_postgres_provider

package postgres

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
)

func FallbackNotNullAsNull(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
	if !ci.IsRowEvent() {
		return ci, typesystem.FallbackDoesNotApplyErr
	}

	fallbackApplied := false
	for i := 0; i < len(ci.TableSchema.Columns()); i++ {
		if ci.TableSchema.Columns()[i].Required {
			ci.TableSchema.Columns()[i].Required = false
			fallbackApplied = true
		}
	}
	if !fallbackApplied {
		return ci, typesystem.FallbackDoesNotApplyErr
	}
	return ci, nil
}

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:       2,
			Picker:   typesystem.ProviderType(ProviderType),
			Function: FallbackNotNullAsNull,
		}
	})
}
