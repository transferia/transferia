//go:build !disable_airbyte_provider

package airbyte

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	// see: https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:   {"integer"},
		schema.TypeInt32:   {},
		schema.TypeInt16:   {},
		schema.TypeInt8:    {},
		schema.TypeUint64:  {},
		schema.TypeUint32:  {},
		schema.TypeUint16:  {},
		schema.TypeUint8:   {},
		schema.TypeFloat32: {},
		schema.TypeFloat64: {"number"},
		schema.TypeBytes:   {},
		schema.TypeString:  {"time_without_timezone", "time_with_timezone", "string"},
		schema.TypeBoolean: {"boolean"},
		schema.TypeAny: {
			"object", "array", typesystem.RestPlaceholder,
		},
		schema.TypeDate:      {"date"},
		schema.TypeDatetime:  {"date-time"},
		schema.TypeTimestamp: {"timestamp", "timestamp_with_timezone", "timestamp_without_timezone"},
	})
}
