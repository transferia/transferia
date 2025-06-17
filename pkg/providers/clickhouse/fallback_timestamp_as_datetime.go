//go:build !disable_clickhouse_provider

package clickhouse

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.AddFallbackTargetFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:     1,
			Picker: typesystem.ProviderType(ProviderType),
			Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
				if !ci.IsRowEvent() {
					switch ci.Kind {
					case abstract.InitTableLoad, abstract.DoneTableLoad,
						abstract.InitShardedTableLoad, abstract.DoneShardedTableLoad:
						// perform fallback
					default:
						return ci, typesystem.FallbackDoesNotApplyErr
					}
				}

				fallbackApplied := false
				for i := 0; i < len(ci.TableSchema.Columns()); i++ {
					switch ci.TableSchema.Columns()[i].DataType {
					case schema.TypeTimestamp.String():
						fallbackApplied = true
						ci.TableSchema.Columns()[i].DataType = schema.TypeDatetime.String()
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
