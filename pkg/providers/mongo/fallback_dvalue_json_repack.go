//go:build !disable_mongo_provider

package mongo

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
)

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:     6,
			Picker: typesystem.ProviderType(ProviderType),
			Function: func(ci *abstract.ChangeItem) (resCI *abstract.ChangeItem, err error) {
				if !ci.IsRowEvent() {
					// skip non-row kinds
					return ci, typesystem.FallbackDoesNotApplyErr
				}
				if ci.Kind == abstract.DeleteKind {
					// skip delete kinds as well
					return ci, typesystem.FallbackDoesNotApplyErr
				}

				fallbackApplied := false
				for i := 0; i < len(ci.TableSchema.Columns()); i++ {
					switch ci.TableSchema.Columns()[i].ColumnName {
					case Document:
						if i >= len(ci.ColumnValues) {
							return ci, xerrors.Errorf(
								"index value %d out of bounds %d for change item of kind %v with schema %v",
								i, len(ci.ColumnValues), ci.Kind, ci.TableSchema)
						}
						val, ok := ci.ColumnValues[i].(DValue)
						if !ok {
							return ci, xerrors.Errorf("expected column value of type %T, actual type: %T", *new(DValue), val)
						}
						fallbackApplied = true
						newVal, err := val.RepackValue()
						if err != nil {
							return ci, xerrors.Errorf("cannot repack value: %w", err)
						}
						ci.ColumnValues[i] = newVal
						ci.TableSchema.Columns()[i].OriginalType = "mongo:bson_raw" // mark no need for repack in abstract.Restore
					default:
						// do nothing
					}
				}
				if !fallbackApplied {
					return ci, typesystem.FallbackDoesNotApplyErr
				}
				return ci, nil
			},
		}
	})
}
