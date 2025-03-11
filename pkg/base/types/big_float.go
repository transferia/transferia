package types

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type BigFloatValue interface {
	base.Value
	BigFloatValue() *string
}

type BigFloatType struct {
	precision int
}

func NewBigFloatType(precision int) *BigFloatType {
	return &BigFloatType{
		precision: precision,
	}
}

func (typ *BigFloatType) Cast(value base.Value) (BigFloatValue, error) {
	BigFloatValue, ok := value.(BigFloatValue)
	if ok {
		return BigFloatValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to BigFloatValue", value)
	}
}

func (typ *BigFloatType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *BigFloatType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeString, nil
}

func (typ *BigFloatType) Precision() int {
	return typ.precision
}

type DefaultBigFloatValue struct {
	column base.Column
	value  *string
}

func NewDefaultBigFloatValue(value *string, column base.Column) *DefaultBigFloatValue {
	return &DefaultBigFloatValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultBigFloatValue) Column() base.Column {
	return value.column
}

func (value *DefaultBigFloatValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultBigFloatValue) BigFloatValue() *string {
	return value.value
}

func (value *DefaultBigFloatValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
