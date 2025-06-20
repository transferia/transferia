//go:build !disable_yt_provider

package types

import (
	"math"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/types"
	"github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"go.ytsaurus.tech/yt/go/schema"
)

func castInt64Based(raw int64, col table.YtColumn) (base.Value, error) {
	switch t := col.YtType().(schema.Type); t {
	case schema.TypeInt8:
		v := int8(raw)
		return types.NewDefaultInt8Value(&v, col), nil
	case schema.TypeInt16:
		v := int16(raw)
		return types.NewDefaultInt16Value(&v, col), nil
	case schema.TypeInt32:
		v := int32(raw)
		return types.NewDefaultInt32Value(&v, col), nil
	case schema.TypeInt64:
		return types.NewDefaultInt64Value(&raw, col), nil
	case schema.TypeInterval:
		// Golang's duration is int64 in nsecs, yt's is in mircrosecs
		if raw > math.MaxInt64/1000 || raw < math.MinInt64/1000 {
			return nil, xerrors.Errorf("interval %d doesn't fit into Duration", raw)
		}
		v := time.Duration(raw * 1000)
		return types.NewDefaultIntervalValue(&v, col), nil
	default:
		return nil, xerrors.Errorf("unsupported int-based type %s", t)
	}
}

func castUInt64Based(raw uint64, col table.YtColumn) (base.Value, error) {
	switch t := col.YtType().(schema.Type); t {
	case schema.TypeUint8:
		v := uint8(raw)
		return types.NewDefaultUInt8Value(&v, col), nil
	case schema.TypeUint16:
		v := uint16(raw)
		return types.NewDefaultUInt16Value(&v, col), nil
	case schema.TypeUint32:
		v := uint32(raw)
		return types.NewDefaultUInt32Value(&v, col), nil
	case schema.TypeUint64:
		return types.NewDefaultUInt64Value(&raw, col), nil
	case schema.TypeDate:
		v := time.Date(1970, 1, 1+int(raw), 0, 0, 0, 0, time.UTC)
		return types.NewDefaultDateValue(&v, col), nil
	case schema.TypeDatetime:
		v := time.Date(1970, 1, 1, 0, 0, int(raw), 0, time.UTC)
		return types.NewDefaultDateTimeValue(&v, col), nil
	case schema.TypeTimestamp:
		msec := int(raw % 1e+6)
		sec := int(raw / 1e+6)
		v := time.Date(1970, 1, 1, 0, 0, sec, msec*1000, time.UTC)
		return types.NewDefaultTimestampValue(&v, col), nil
	default:
		return nil, xerrors.Errorf("unsupported uint-based %s", t)
	}
}

func castFloat64Based(raw float64, col table.YtColumn) (base.Value, error) {
	switch t := col.YtType().(schema.Type); t {
	case schema.TypeFloat32:
		v := float32(raw)
		return types.NewDefaultFloatValue(&v, col), nil
	case schema.TypeFloat64:
		return types.NewDefaultDoubleValue(&raw, col), nil
	default:
		return nil, xerrors.Errorf("unsupported float-based %s", t)
	}
}

func castNullValue(col table.YtColumn) (base.Value, error) {
	switch t := col.YtType().(schema.Type); t {
	case schema.TypeInt8:
		return types.NewDefaultInt8Value(nil, col), nil
	case schema.TypeInt16:
		return types.NewDefaultInt16Value(nil, col), nil
	case schema.TypeInt32:
		return types.NewDefaultInt32Value(nil, col), nil
	case schema.TypeInt64:
		return types.NewDefaultInt64Value(nil, col), nil
	case schema.TypeInterval:
		return types.NewDefaultIntervalValue(nil, col), nil
	case schema.TypeUint8:
		return types.NewDefaultUInt8Value(nil, col), nil
	case schema.TypeUint16:
		return types.NewDefaultUInt16Value(nil, col), nil
	case schema.TypeUint32:
		return types.NewDefaultUInt32Value(nil, col), nil
	case schema.TypeUint64:
		return types.NewDefaultUInt64Value(nil, col), nil
	case schema.TypeDate:
		return types.NewDefaultDateValue(nil, col), nil
	case schema.TypeDatetime:
		return types.NewDefaultDateTimeValue(nil, col), nil
	case schema.TypeTimestamp:
		return types.NewDefaultTimestampValue(nil, col), nil
	case schema.TypeFloat32:
		return types.NewDefaultFloatValue(nil, col), nil
	case schema.TypeFloat64:
		return types.NewDefaultDoubleValue(nil, col), nil
	case schema.TypeBoolean:
		return types.NewDefaultBoolValue(nil, col), nil
	case schema.TypeAny:
		return types.NewDefaultJSONValue(nil, col), nil
	case schema.TypeBytes, schema.TypeString:
		return types.NewDefaultStringValue(nil, col), nil
	default:
		return nil, xerrors.Errorf("unsupported nullable type %s", t)
	}
}

func castPrimitive(raw interface{}, col table.YtColumn) (base.Value, error) {
	if raw == nil {
		if !col.Nullable() {
			return nil, xerrors.Errorf("unexpected null value in column %s", col.FullName())
		}
		val, err := castNullValue(col)
		if err != nil {
			return nil, xerrors.Errorf("error casting null value for column %s: %w", col.FullName(), err)
		}
		return val, nil
	}
	switch t := col.YtType().(schema.Type); t {
	case schema.TypeInt8, schema.TypeInt16, schema.TypeInt32, schema.TypeInt64, schema.TypeInterval:
		v, ok := raw.(int64)
		if !ok {
			return nil, xerrors.Errorf("expected int64 as %s raw value, got %T", t, raw)
		}
		val, err := castInt64Based(v, col)
		if err != nil {
			return nil, xerrors.Errorf("unable to cast int-based value: %w", err)
		}
		return val, nil
	case schema.TypeUint8, schema.TypeUint16, schema.TypeUint32, schema.TypeUint64,
		schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		v, ok := raw.(uint64)
		if !ok {
			return nil, xerrors.Errorf("expected uint64 as %s raw value, got %T", t, raw)
		}
		val, err := castUInt64Based(v, col)
		if err != nil {
			return nil, xerrors.Errorf("unable to cast uint-based value: %w", err)
		}
		return val, nil
	case schema.TypeFloat32, schema.TypeFloat64:
		v, ok := raw.(float64)
		if !ok {
			return nil, xerrors.Errorf("expected float64 as %s raw value, got %T", t, raw)
		}
		val, err := castFloat64Based(v, col)
		if err != nil {
			return nil, xerrors.Errorf("unable to cast float-based value: %w", err)
		}
		return val, nil
	case schema.TypeBoolean:
		v, ok := raw.(bool)
		if !ok {
			return nil, xerrors.Errorf("expected bool as %s raw value, got %T", t, raw)
		}
		return types.NewDefaultBoolValue(&v, col), nil
	case schema.TypeAny:
		return types.NewDefaultJSONValue(raw, col), nil
	case schema.TypeBytes:
		v, ok := raw.(string)
		if !ok {
			return nil, xerrors.Errorf("expected bytes as %s raw value, got %T", t, raw)
		}
		vb := []byte(v)
		return types.NewDefaultBytesValue(vb, col), nil

	case schema.TypeString:
		v, ok := raw.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string as %s raw value, got %T", t, raw)
		}
		return types.NewDefaultStringValue(&v, col), nil
	default:
		return nil, xerrors.Errorf("unsupported primitive type %s", t)
	}
}

func Cast(raw interface{}, colRaw base.Column) (base.Value, error) {
	col, ok := colRaw.(table.YtColumn)
	if !ok {
		return nil, xerrors.Errorf("expected YT column, got %T", colRaw)
	}
	switch col.YtType().(type) {
	case schema.Type:
		return castPrimitive(raw, col)
	case schema.List, schema.Struct, schema.Tuple, schema.Variant, schema.Dict, schema.Tagged:
		return types.NewDefaultJSONValue(raw, col), nil
	default:
		return nil, xerrors.Errorf("unsupported type %T", col.YtType())
	}
}

func CastPrimitiveToOldValue(raw interface{}, ytType schema.ComplexType) (interface{}, error) {
	//nolint:exhaustivestruct
	col := table.NewColumn("", nil, ytType, schema.Column{}, false)
	casted, err := castPrimitive(raw, col) // castPrimitive needs only ytType when casting primitive types.
	if err != nil {
		return nil, xerrors.Errorf("unable to cast primitive: %w", err)
	}
	return casted.ToOldValue()
}
