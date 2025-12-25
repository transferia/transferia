package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type DecimalValue interface {
	base.Value
	DecimalValue() *string
}

type DecimalType struct {
	precision int
	scale     int
}

func NewDecimalType(precision int, scale int) *DecimalType {
	return &DecimalType{
		precision: precision,
		scale:     scale,
	}
}

func (typ *DecimalType) Cast(value base.Value) (DecimalValue, error) {
	decimalValue, ok := value.(DecimalValue)
	if ok {
		return decimalValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to DecimalValue", value)
	}
}

func (typ *DecimalType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *DecimalType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeString, nil
}

func (typ *DecimalType) Precision() int {
	return typ.precision
}

func (typ *DecimalType) Scale() int {
	return typ.scale
}

type DefaultDecimalValue struct {
	column base.Column
	value  *string
}

func NewDefaultDecimalValue(value *string, column base.Column) *DefaultDecimalValue {
	return &DefaultDecimalValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultDecimalValue) Column() base.Column {
	return value.column
}

func (value *DefaultDecimalValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultDecimalValue) DecimalValue() *string {
	return value.value
}

func (value *DefaultDecimalValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
