//go:build !disable_clickhouse_provider

package clickhouse

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:     {"Int64"},
		schema.TypeInt32:     {"Int32"},
		schema.TypeInt16:     {"Int16"},
		schema.TypeInt8:      {"Int8"},
		schema.TypeUint64:    {"UInt64"},
		schema.TypeUint32:    {"UInt32"},
		schema.TypeUint16:    {"UInt16"},
		schema.TypeUint8:     {"UInt8"},
		schema.TypeFloat32:   {},
		schema.TypeFloat64:   {"Float64"},
		schema.TypeBytes:     {"FixedString", "String"},
		schema.TypeString:    {"IPv4", "IPv6", "Enum8", "Enum16"},
		schema.TypeBoolean:   {},
		schema.TypeAny:       {typesystem.RestPlaceholder},
		schema.TypeDate:      {"Date"},
		schema.TypeDatetime:  {"DateTime"},
		schema.TypeTimestamp: {"DateTime64"},
	})
	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     "Int64",
		schema.TypeInt32:     "Int32",
		schema.TypeInt16:     "Int16",
		schema.TypeInt8:      "Int8",
		schema.TypeUint64:    "UInt64",
		schema.TypeUint32:    "UInt32",
		schema.TypeUint16:    "UInt16",
		schema.TypeUint8:     "UInt8",
		schema.TypeFloat32:   "Float64",
		schema.TypeFloat64:   "Float64",
		schema.TypeBytes:     "String",
		schema.TypeString:    "String",
		schema.TypeBoolean:   "UInt8",
		schema.TypeAny:       "String",
		schema.TypeDate:      "Date",
		schema.TypeDatetime:  "DateTime",
		schema.TypeTimestamp: "DateTime64(9)",
	})
}
