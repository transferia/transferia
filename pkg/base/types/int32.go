package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type Int32Value interface {
	base.Value
	Int32Value() *int32
}

type Int32Type struct {
}

func NewInt32Type() *Int32Type {
	return &Int32Type{}
}

func (typ *Int32Type) Cast(value base.Value) (Int32Value, error) {
	int32Value, ok := value.(Int32Value)
	if ok {
		return int32Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to Int32Value", value)
	}
}

func (typ *Int32Type) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *Int32Type) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeInt32, nil
}

type DefaultInt32Value struct {
	column base.Column
	value  *int32
}

func NewDefaultInt32Value(value *int32, column base.Column) *DefaultInt32Value {
	return &DefaultInt32Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultInt32Value) Column() base.Column {
	return value.column
}

func (value *DefaultInt32Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultInt32Value) Int32Value() *int32 {
	return value.value
}

func (value *DefaultInt32Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
