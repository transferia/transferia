package types

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type FloatValue interface {
	base.Value
	FloatValue() *float32
}

type FloatType struct {
}

func NewFloatType() *FloatType {
	return &FloatType{}
}

func (typ *FloatType) Cast(value base.Value) (FloatValue, error) {
	floatValue, ok := value.(FloatValue)
	if ok {
		return floatValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to FloatValue", value)
	}
}

func (typ *FloatType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *FloatType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeFloat32, nil
}

type DefaultFloatValue struct {
	column base.Column
	value  *float32
}

func NewDefaultFloatValue(value *float32, column base.Column) *DefaultFloatValue {
	return &DefaultFloatValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultFloatValue) Column() base.Column {
	return value.column
}

func (value *DefaultFloatValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultFloatValue) FloatValue() *float32 {
	return value.value
}

func (value *DefaultFloatValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
