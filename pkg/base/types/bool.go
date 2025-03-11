package types

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type BoolValue interface {
	base.Value
	BoolValue() *bool
}

type BoolType struct {
}

func NewBoolType() *BoolType {
	return &BoolType{}
}

func (typ *BoolType) Cast(value base.Value) (BoolValue, error) {
	boolValue, ok := value.(BoolValue)
	if ok {
		return boolValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to BoolValue", value)
	}
}

func (typ *BoolType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *BoolType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeBoolean, nil
}

type DefaultBoolValue struct {
	column base.Column
	value  *bool
}

func NewDefaultBoolValue(value *bool, column base.Column) *DefaultBoolValue {
	return &DefaultBoolValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultBoolValue) Column() base.Column {
	return value.column
}

func (value *DefaultBoolValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultBoolValue) BoolValue() *bool {
	return value.value
}

func (value *DefaultBoolValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
