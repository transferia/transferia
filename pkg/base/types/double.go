package types

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/util/castx"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type DoubleValue interface {
	base.Value
	DoubleValue() *float64
}

type DoubleType struct{}

func NewDoubleType() *DoubleType {
	return &DoubleType{}
}

func (typ *DoubleType) Cast(value base.Value) (DoubleValue, error) {
	doubleValue, ok := value.(DoubleValue)
	if ok {
		return doubleValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to DoubleValue", value)
	}
}

func (typ *DoubleType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *DoubleType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeFloat64, nil
}

type DefaultDoubleValue struct {
	column base.Column
	value  *float64
}

func NewDefaultDoubleValue(value *float64, column base.Column) *DefaultDoubleValue {
	return &DefaultDoubleValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultDoubleValue) Column() base.Column {
	return value.column
}

func (value *DefaultDoubleValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultDoubleValue) DoubleValue() *float64 {
	return value.value
}

func (value *DefaultDoubleValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return castx.ToJSONNumberE(*value.value)
}
