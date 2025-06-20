//go:build !disable_delta_provider

package delta

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/providers/delta/types"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:     new(types.LongType).Aliases(),
		schema.TypeInt32:     new(types.IntegerType).Aliases(),
		schema.TypeInt16:     new(types.ShortType).Aliases(),
		schema.TypeInt8:      new(types.ByteType).Aliases(),
		schema.TypeUint64:    {},
		schema.TypeUint32:    {},
		schema.TypeUint16:    {},
		schema.TypeUint8:     {},
		schema.TypeFloat32:   {new(types.DoubleType).Name()},
		schema.TypeFloat64:   new(types.FloatType).Aliases(),
		schema.TypeBytes:     {new(types.BinaryType).Name()},
		schema.TypeString:    {new(types.StringType).Name()},
		schema.TypeBoolean:   {new(types.BooleanType).Name()},
		schema.TypeDate:      {new(types.DateType).Name()},
		schema.TypeDatetime:  {},
		schema.TypeTimestamp: {new(types.TimestampType).Name()},
		schema.TypeInterval:  {},
		schema.TypeAny: {
			typesystem.RestPlaceholder,
		},
	})
}
