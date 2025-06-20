//go:build !disable_yt_provider

package types

import (
	"math"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/types"
	"go.ytsaurus.tech/yt/go/schema"
)

func resolvePrimitive(t schema.Type) (base.Type, error) {
	switch t {
	case schema.TypeInt8:
		return types.NewInt8Type(), nil
	case schema.TypeInt16:
		return types.NewInt16Type(), nil
	case schema.TypeInt32:
		return types.NewInt32Type(), nil
	case schema.TypeInt64:
		return types.NewInt64Type(), nil
	case schema.TypeUint8:
		return types.NewUInt8Type(), nil
	case schema.TypeUint16:
		return types.NewUInt16Type(), nil
	case schema.TypeUint32:
		return types.NewUInt32Type(), nil
	case schema.TypeUint64:
		return types.NewUInt64Type(), nil
	case schema.TypeBytes:
		return types.NewBytesType(), nil
	case schema.TypeString:
		return types.NewStringType(math.MaxInt64), nil
	case schema.TypeBoolean:
		return types.NewBoolType(), nil
	case schema.TypeFloat32:
		return types.NewFloatType(), nil
	case schema.TypeFloat64:
		return types.NewDoubleType(), nil
	case schema.TypeDate:
		return types.NewDateType(), nil
	case schema.TypeDatetime:
		return types.NewDateTimeType(), nil
	case schema.TypeInterval:
		return types.NewIntervalType(), nil
	case schema.TypeTimestamp:
		return types.NewTimestampType(6), nil
	case schema.TypeAny:
		return types.NewJSONType(), nil
	default:
		return nil, xerrors.Errorf("unknown yt primitive type %s", t)
	}
}

func UnwrapOptional(ytType schema.ComplexType) (schema.ComplexType, bool) {
	if unwrapped, isOptional := ytType.(schema.Optional); isOptional {
		v, _ := UnwrapOptional(unwrapped.Item)
		return v, true
	}
	return ytType, false
}

func Resolve(typ schema.ComplexType) (base.Type, error) {
	switch t := typ.(type) {
	case schema.Type:
		if result, err := resolvePrimitive(t); err != nil {
			return nil, xerrors.Errorf("cannot resolve yt primitive type: %w", err)
		} else {
			return result, nil
		}
	case schema.List, schema.Struct, schema.Tuple, schema.Variant, schema.Dict, schema.Tagged:
		return types.NewJSONType(), nil
	default:
		return nil, xerrors.Errorf("yt type %T is not supported", typ)
	}
}
