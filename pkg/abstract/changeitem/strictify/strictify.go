package strictify

import (
	"math"

	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/dterrors"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/transferia/transferia/pkg/util/generics"
	"go.ytsaurus.tech/yt/go/schema"
)

// Strictify converts Values inside an item to their strict representations in-place. To determine the type to which to convert the value of each column, the given set of column schemas is used.
//
// The method returns different classes of errors, some of which can be handled. See implementation for details. If an error returned is not `nil`, all values remain unchanged.
func Strictify(c *changeitem.ChangeItem, tableSchema changeitem.FastTableSchema) error {
	if err := c.EnsureSanity(); err != nil {
		return xerrors.Errorf("cannot canonize an insane ChangeItem: %w", err)
	}

	strictColumnValues := make([]any, len(c.ColumnNames))
	for i, columnName := range c.ColumnNames {
		value := &c.ColumnValues[i]
		columnSchema, ok := tableSchema[changeitem.ColumnName(columnName)]
		if !ok {
			strictColumnValues[i] = *value
			continue
		}
		var err error
		strictColumnValues[i], err = strictifyValue(value, &columnSchema)
		if err != nil {
			return xerrors.Errorf("failed to strictify the value of column [%d] %q: %w", i, columnName, err)
		}
	}

	for i := range c.ColumnNames {
		c.ColumnValues[i] = strictColumnValues[i]
	}
	return nil
}

func strictifyValue(value *any, valueSchema *changeitem.ColSchema) (any, error) {
	if value == nil {
		return nil, dterrors.NewFatalError(xerrors.New("faulty call of strictifyValue with a null pointer to value"))
	}
	if valueSchema == nil {
		return nil, dterrors.NewFatalError(xerrors.New("cannot strictify a value without a schema"))
	}

	if *value == nil {
		return nil, nil
	}

	targetType := schema.Type(valueSchema.DataType)
	switch targetType {
	case schema.TypeBoolean:
		v, err := cast.ToBoolE(*value)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeInt8:
		v, err := toSignedInt(*value, math.MinInt8, math.MaxInt8, cast.ToInt8E)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeInt16:
		v, err := toSignedInt(*value, math.MinInt16, math.MaxInt16, cast.ToInt16E)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeInt32:
		v, err := toSignedInt(*value, math.MinInt32, math.MaxInt32, cast.ToInt32E)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeInt64:
		v, err := toSignedInt(*value, math.MinInt64, math.MaxInt64, cast.ToInt64E)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeUint8:
		v, err := toUnsignedInt(*value, math.MaxUint8, cast.ToUint8E)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeUint16:
		v, err := toUnsignedInt(*value, math.MaxUint16, cast.ToUint16E)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeUint32:
		v, err := toUnsignedInt(*value, math.MaxUint32, cast.ToUint32E)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeUint64:
		v, err := toUnsignedInt(*value, math.MaxUint64, cast.ToUint64E)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeFloat32:
		v, err := cast.ToFloat32E(*value)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeFloat64:
		v, err := castx.ToJSONNumberE(*value)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeBytes:
		v, err := castx.ToByteSliceE(*value)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeString:
		v, err := castx.ToStringE(*value)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		v, err := cast.ToTimeE(*value)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeInterval:
		v, err := cast.ToDurationE(*value)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	case schema.TypeAny:
		v, err := castx.ToJSONMarshallableE(*value)
		if err != nil {
			return nil, NewStrictifyError(valueSchema, targetType, err)
		}
		return v, nil
	default:
		return nil, dterrors.NewFatalError(xerrors.Errorf("cannot strictify value of unknown type %s", valueSchema.DataType))
	}
}

func toSignedInt[T generics.SignedInt](v any, limitLow int64, limitHigh int64, castFn func(any) (T, error)) (T, error) {
	result, err := castFn(v)
	if err != nil {
		return result, xerrors.Errorf("cannot cast to int: %w", err)
	}
	v64 := cast.ToInt64(v)
	if v64 < limitLow || v64 > limitHigh {
		return result, StrictifyRangeError
	}
	return result, nil
}

func toUnsignedInt[T generics.UnsignedInt](v any, limitHigh uint64, castFn func(any) (T, error)) (T, error) {
	result, err := castFn(v)
	if err != nil {
		return result, xerrors.Errorf("cannot cast to uint: %w", err)
	}
	v64 := cast.ToUint64(v)
	if v64 > limitHigh {
		return result, StrictifyRangeError
	}
	return result, nil
}
