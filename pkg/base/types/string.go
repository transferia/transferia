package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type StringValue interface {
	base.Value
	StringValue() *string
}

type StringType struct {
	length int
}

func NewStringType(length int) *StringType {
	return &StringType{
		length: length,
	}
}

func (typ *StringType) Cast(value base.Value) (StringValue, error) {
	stringValue, ok := value.(StringValue)
	if ok {
		return stringValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to StringValue", value)
	}
}

func (typ *StringType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *StringType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeString, nil
}

func (typ *StringType) Length() int {
	return typ.length
}

type DefaultStringValue struct {
	column base.Column
	value  *string
}

func NewDefaultStringValue(value *string, column base.Column) *DefaultStringValue {
	return &DefaultStringValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultStringValue) Column() base.Column {
	return value.column
}

func (value *DefaultStringValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultStringValue) StringValue() *string {
	return value.value
}

func (value *DefaultStringValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
