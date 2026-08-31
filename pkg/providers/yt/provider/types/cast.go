package types

import (
	"math"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/castx"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

// CastPrimitiveToOldValue normalizes a raw value produced by the YSON/JSON
// deserialization layer to the exact Go type that corresponds to ytType, so a
// map[T comparable]any downstream (see yt_dict.upsertPrimitiveToDict) can
// accept it as a typed key.
//
// The mapping preserves the historical ToOldValue semantics of the previous
// value wrappers (integers narrow to their target width, TypeFloat64 becomes
// json.Number, time-like uint64s decode into time.Time in UTC, TypeInterval
// int64 microseconds decode into time.Duration, TypeAny is passed through,
// TypeNull always yields nil).
func CastPrimitiveToOldValue(raw interface{}, ytType ytschema.ComplexType) (interface{}, error) {
	primitive, ok := ytType.(ytschema.Type)
	if !ok {
		return nil, xerrors.Errorf("expected primitive yt type, got %T", ytType)
	}
	if primitive == ytschema.TypeNull {
		return nil, nil
	}
	if raw == nil {
		return nil, nil
	}
	switch primitive {
	case ytschema.TypeInt8:
		v, ok := raw.(int64)
		if !ok {
			return nil, xerrors.Errorf("expected int64 as %s raw value, got %T", primitive, raw)
		}
		return int8(v), nil
	case ytschema.TypeInt16:
		v, ok := raw.(int64)
		if !ok {
			return nil, xerrors.Errorf("expected int64 as %s raw value, got %T", primitive, raw)
		}
		return int16(v), nil
	case ytschema.TypeInt32:
		v, ok := raw.(int64)
		if !ok {
			return nil, xerrors.Errorf("expected int64 as %s raw value, got %T", primitive, raw)
		}
		return int32(v), nil
	case ytschema.TypeInt64:
		v, ok := raw.(int64)
		if !ok {
			return nil, xerrors.Errorf("expected int64 as %s raw value, got %T", primitive, raw)
		}
		return v, nil
	case ytschema.TypeUint8:
		v, ok := raw.(uint64)
		if !ok {
			return nil, xerrors.Errorf("expected uint64 as %s raw value, got %T", primitive, raw)
		}
		return uint8(v), nil
	case ytschema.TypeUint16:
		v, ok := raw.(uint64)
		if !ok {
			return nil, xerrors.Errorf("expected uint64 as %s raw value, got %T", primitive, raw)
		}
		return uint16(v), nil
	case ytschema.TypeUint32:
		v, ok := raw.(uint64)
		if !ok {
			return nil, xerrors.Errorf("expected uint64 as %s raw value, got %T", primitive, raw)
		}
		return uint32(v), nil
	case ytschema.TypeUint64:
		v, ok := raw.(uint64)
		if !ok {
			return nil, xerrors.Errorf("expected uint64 as %s raw value, got %T", primitive, raw)
		}
		return v, nil
	case ytschema.TypeFloat32:
		v, ok := raw.(float64)
		if !ok {
			return nil, xerrors.Errorf("expected float64 as %s raw value, got %T", primitive, raw)
		}
		return float32(v), nil
	case ytschema.TypeFloat64:
		v, ok := raw.(float64)
		if !ok {
			return nil, xerrors.Errorf("expected float64 as %s raw value, got %T", primitive, raw)
		}
		// The previous DefaultDoubleValue.ToOldValue() returned json.Number
		// via castx.ToJSONNumberE; preserve that so map[json.Number]any dict
		// keys continue to work in yt_dict.upsertPrimitiveToDict.
		return castx.ToJSONNumberE(v)
	case ytschema.TypeBoolean:
		v, ok := raw.(bool)
		if !ok {
			return nil, xerrors.Errorf("expected bool as %s raw value, got %T", primitive, raw)
		}
		return v, nil
	case ytschema.TypeString:
		v, ok := raw.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string as %s raw value, got %T", primitive, raw)
		}
		return v, nil
	case ytschema.TypeBytes:
		v, ok := raw.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string as %s raw value, got %T", primitive, raw)
		}
		return []byte(v), nil
	case ytschema.TypeDate:
		v, ok := raw.(uint64)
		if !ok {
			return nil, xerrors.Errorf("expected uint64 as %s raw value, got %T", primitive, raw)
		}
		return time.Date(1970, 1, 1+int(v), 0, 0, 0, 0, time.UTC), nil
	case ytschema.TypeDatetime:
		v, ok := raw.(uint64)
		if !ok {
			return nil, xerrors.Errorf("expected uint64 as %s raw value, got %T", primitive, raw)
		}
		return time.Date(1970, 1, 1, 0, 0, int(v), 0, time.UTC), nil
	case ytschema.TypeTimestamp:
		v, ok := raw.(uint64)
		if !ok {
			return nil, xerrors.Errorf("expected uint64 as %s raw value, got %T", primitive, raw)
		}
		msec := int(v % 1e+6)
		sec := int(v / 1e+6)
		return time.Date(1970, 1, 1, 0, 0, sec, msec*1000, time.UTC), nil
	case ytschema.TypeInterval:
		v, ok := raw.(int64)
		if !ok {
			return nil, xerrors.Errorf("expected int64 as %s raw value, got %T", primitive, raw)
		}
		// YT interval is int64 microseconds, Go's time.Duration is int64 nanoseconds.
		// Preserve the historical overflow guard from the earlier cast path.
		if v > math.MaxInt64/1000 || v < math.MinInt64/1000 {
			return nil, xerrors.Errorf("interval %d doesn't fit into Duration", v)
		}
		return time.Duration(v) * time.Microsecond, nil
	case ytschema.TypeAny:
		return raw, nil
	default:
		return nil, xerrors.Errorf("unsupported primitive type %s", primitive)
	}
}
