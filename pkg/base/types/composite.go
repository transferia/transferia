package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type CompositeValue interface {
	base.Value
	CompositeValue() interface{}
}

type CompositeType struct {
}

func NewCompositeType() *CompositeType {
	return &CompositeType{}
}

func (typ *CompositeType) Cast(value base.Value) (CompositeValue, error) {
	compositeValue, ok := value.(CompositeValue)
	if ok {
		return compositeValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to AnyValue", value)
	}
}

func (typ *CompositeType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *CompositeType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeString, nil
}

type DefaultCompositeValue struct {
	column base.Column
	value  interface{}
}

func NewDefaultCompositeValue(value interface{}, column base.Column) *DefaultCompositeValue {
	return &DefaultCompositeValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultCompositeValue) Column() base.Column {
	return value.column
}

func (value *DefaultCompositeValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return value.value
}

func (value *DefaultCompositeValue) CompositeValue() interface{} {
	return value.value
}

func (value *DefaultCompositeValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return value.value, nil
}
