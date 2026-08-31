package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

// resolvePrimitive round-trips a YT primitive type to itself, or maps the
// "opaque" primitives (Any, Null) to TypeAny so downstream schema layers can
// treat them uniformly with composite types.
func resolvePrimitive(t ytschema.Type) (ytschema.Type, error) {
	switch t {
	case ytschema.TypeInt8, ytschema.TypeInt16, ytschema.TypeInt32, ytschema.TypeInt64,
		ytschema.TypeUint8, ytschema.TypeUint16, ytschema.TypeUint32, ytschema.TypeUint64,
		ytschema.TypeFloat32, ytschema.TypeFloat64,
		ytschema.TypeBytes, ytschema.TypeString, ytschema.TypeBoolean,
		ytschema.TypeDate, ytschema.TypeDatetime, ytschema.TypeInterval, ytschema.TypeTimestamp:
		return t, nil
	case ytschema.TypeAny, ytschema.TypeNull:
		return ytschema.TypeAny, nil
	default:
		return "", xerrors.Errorf("unknown yt primitive type %s", t)
	}
}

func UnwrapOptional(ytType ytschema.ComplexType) (ytschema.ComplexType, bool) {
	if unwrapped, isOptional := ytType.(ytschema.Optional); isOptional {
		v, _ := UnwrapOptional(unwrapped.Item)
		return v, true
	}
	return ytType, false
}

// Resolve reduces a YT complex type to a single primitive that the passive
// provider layer works with. Primitives round-trip via resolvePrimitive;
// composite types (List, Struct, Tuple, Variant, Dict, Tagged) collapse to
// TypeAny, since they are wire-encoded as opaque YSON on the Skiff/JSON path.
func Resolve(typ ytschema.ComplexType) (ytschema.Type, error) {
	switch t := typ.(type) {
	case ytschema.Type:
		result, err := resolvePrimitive(t)
		if err != nil {
			return "", xerrors.Errorf("cannot resolve yt primitive type: %w", err)
		}
		return result, nil
	case ytschema.List, ytschema.Struct, ytschema.Tuple, ytschema.Variant, ytschema.Dict, ytschema.Tagged:
		return ytschema.TypeAny, nil
	default:
		return "", xerrors.Errorf("yt type %T is not supported", typ)
	}
}
