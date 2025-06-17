//go:build !disable_mysql_provider

package replication

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/strict"
	"go.ytsaurus.tech/yt/go/schema"
)

func UnmarshalHomo(value any, colSchema *abstract.ColSchema, _ *time.Location) (any, error) {
	if colSchema.DataType == string(schema.TypeAny) {
		return strict.Expected[string](value, castToAny)
	}
	return value, nil
}
