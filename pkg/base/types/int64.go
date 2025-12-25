package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type Int64Value interface {
	base.Value
	Int64Value() *int64
}

type Int64Type struct {
}

func NewInt64Type() *Int64Type {
	return &Int64Type{}
}

func (typ *Int64Type) Cast(value base.Value) (Int64Value, error) {
	int64Value, ok := value.(Int64Value)
	if ok {
		return int64Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to Int64Value", value)
	}
}

func (typ *Int64Type) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *Int64Type) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeInt64, nil
}

type DefaultInt64Value struct {
	column base.Column
	value  *int64
}

func NewDefaultInt64Value(value *int64, column base.Column) *DefaultInt64Value {
	return &DefaultInt64Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultInt64Value) Column() base.Column {
	return value.column
}

func (value *DefaultInt64Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultInt64Value) Int64Value() *int64 {
	return value.value
}

func (value *DefaultInt64Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
