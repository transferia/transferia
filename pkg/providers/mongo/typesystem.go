//go:build !disable_mongo_provider

package mongo

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:     {},
		schema.TypeInt32:     {},
		schema.TypeInt16:     {},
		schema.TypeInt8:      {},
		schema.TypeUint64:    {},
		schema.TypeUint32:    {},
		schema.TypeUint16:    {},
		schema.TypeUint8:     {},
		schema.TypeFloat32:   {},
		schema.TypeFloat64:   {},
		schema.TypeBytes:     {},
		schema.TypeString:    {"bson_id"},
		schema.TypeBoolean:   {},
		schema.TypeAny:       {"bson"},
		schema.TypeDate:      {},
		schema.TypeDatetime:  {},
		schema.TypeTimestamp: {},
		schema.TypeInterval:  {},
	})
	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     "bson",
		schema.TypeInt32:     "bson",
		schema.TypeInt16:     "bson",
		schema.TypeInt8:      "bson",
		schema.TypeUint64:    "bson",
		schema.TypeUint32:    "bson",
		schema.TypeUint16:    "bson",
		schema.TypeUint8:     "bson",
		schema.TypeFloat32:   "bson",
		schema.TypeFloat64:   "bson",
		schema.TypeBytes:     "bson",
		schema.TypeString:    "bson",
		schema.TypeBoolean:   "bson",
		schema.TypeAny:       "bson",
		schema.TypeDate:      "bson",
		schema.TypeDatetime:  "bson",
		schema.TypeTimestamp: "bson",
	})
}
