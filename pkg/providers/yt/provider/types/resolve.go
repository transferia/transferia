package types

import (
	"math"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/abstract2/types"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func resolvePrimitive(t ytschema.Type) (abstract2.Type, error) {
	switch t {
	case ytschema.TypeInt8:
		return types.NewInt8Type(), nil
	case ytschema.TypeInt16:
		return types.NewInt16Type(), nil
	case ytschema.TypeInt32:
		return types.NewInt32Type(), nil
	case ytschema.TypeInt64:
		return types.NewInt64Type(), nil
	case ytschema.TypeUint8:
		return types.NewUInt8Type(), nil
	case ytschema.TypeUint16:
		return types.NewUInt16Type(), nil
	case ytschema.TypeUint32:
		return types.NewUInt32Type(), nil
	case ytschema.TypeUint64:
		return types.NewUInt64Type(), nil
	case ytschema.TypeBytes:
		return types.NewBytesType(), nil
	case ytschema.TypeString:
		return types.NewStringType(math.MaxInt64), nil
	case ytschema.TypeBoolean:
		return types.NewBoolType(), nil
	case ytschema.TypeFloat32:
		return types.NewFloatType(), nil
	case ytschema.TypeFloat64:
		return types.NewDoubleType(), nil
	case ytschema.TypeDate:
		return types.NewDateType(), nil
	case ytschema.TypeDatetime:
		return types.NewDateTimeType(), nil
	case ytschema.TypeInterval:
		return types.NewIntervalType(), nil
	case ytschema.TypeTimestamp:
		return types.NewTimestampType(6), nil
	case ytschema.TypeAny, ytschema.TypeNull:
		return types.NewJSONType(), nil
	default:
		return nil, xerrors.Errorf("unknown yt primitive type %s", t)
	}
}

func UnwrapOptional(ytType ytschema.ComplexType) (ytschema.ComplexType, bool) {
	if unwrapped, isOptional := ytType.(ytschema.Optional); isOptional {
		v, _ := UnwrapOptional(unwrapped.Item)
		return v, true
	}
	return ytType, false
}

func Resolve(typ ytschema.ComplexType) (abstract2.Type, error) {
	switch t := typ.(type) {
	case ytschema.Type:
		if result, err := resolvePrimitive(t); err != nil {
			return nil, xerrors.Errorf("cannot resolve yt primitive type: %w", err)
		} else {
			return result, nil
		}
	case ytschema.List, ytschema.Struct, ytschema.Tuple, ytschema.Variant, ytschema.Dict, ytschema.Tagged:
		return types.NewJSONType(), nil
	default:
		return nil, xerrors.Errorf("yt type %T is not supported", typ)
	}
}
