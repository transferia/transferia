//go:build !disable_postgres_provider

package postgres

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

// FallbackBitAsBytes implements backward compatibility for https://st.yandex-team.ru/TM-5445#640b22b5af0c626bd522d179
//
// WARNING: this fallback violates type strictness!
func FallbackBitAsBytes(item *abstract.ChangeItem) (*abstract.ChangeItem, error) {
	fallbackApplied := false

	for i := 0; i < len(item.TableSchema.Columns()); i++ {
		clearedPGType := ClearOriginalType(item.TableSchema.Columns()[i].OriginalType)
		if clearedPGType == "BIT(N)" || clearedPGType == "BIT VARYING(N)" {
			item.TableSchema.Columns()[i].DataType = schema.TypeBytes.String()
			fallbackApplied = true
		}
	}

	if !fallbackApplied {
		return item, typesystem.FallbackDoesNotApplyErr
	}
	return item, nil
}

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:       5,
			Picker:   typesystem.ProviderType(ProviderType),
			Function: FallbackBitAsBytes,
		}
	})
}
