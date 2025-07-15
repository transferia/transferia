package ydb

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
		schema.TypeUint64:    {"Uint64"},
		schema.TypeUint32:    {"Uint32"},
		schema.TypeUint16:    {"Uint16"},
		schema.TypeUint8:     {"Uint8"},
		schema.TypeFloat32:   {"Float"},
		schema.TypeFloat64:   {"Double"},
		schema.TypeBytes:     {"String"},
		schema.TypeString:    {"Utf8", "Decimal", "DyNumber", "Uuid"},
		schema.TypeBoolean:   {"Bool"},
		schema.TypeAny:       {typesystem.RestPlaceholder},
		schema.TypeDate:      {"Date"},
		schema.TypeDatetime:  {"Datetime"},
		schema.TypeTimestamp: {"Timestamp"},
		schema.TypeInterval:  {"Interval"},
	})
	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     "Int64",
		schema.TypeInt32:     "Int32",
		schema.TypeInt16:     "Int32",
		schema.TypeInt8:      "Int32",
		schema.TypeUint64:    "Uint64",
		schema.TypeUint32:    "Uint32",
		schema.TypeUint16:    "Uint32",
		schema.TypeUint8:     "Uint8",
		schema.TypeFloat32:   typesystem.NotSupportedPlaceholder,
		schema.TypeFloat64:   "Double",
		schema.TypeBytes:     "String",
		schema.TypeString:    "Utf8",
		schema.TypeBoolean:   "Bool",
		schema.TypeAny:       "Json",
		schema.TypeDate:      "Date",
		schema.TypeDatetime:  "Datetime",
		schema.TypeTimestamp: "Timestamp",
	})
}
