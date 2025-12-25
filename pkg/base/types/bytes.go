package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type BytesValue interface {
	base.Value
	BytesValue() []byte
}

type BytesType struct {
}

func NewBytesType() *BytesType {
	return &BytesType{}
}

func (typ *BytesType) Cast(value base.Value) (BytesValue, error) {
	bytesValue, ok := value.(BytesValue)
	if ok {
		return bytesValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to BytesValue", value)
	}
}

func (typ *BytesType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *BytesType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeBytes, nil
}

type DefaultBytesValue struct {
	column base.Column
	value  []byte
}

func NewDefaultBytesValue(value []byte, column base.Column) *DefaultBytesValue {
	return &DefaultBytesValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultBytesValue) Column() base.Column {
	return value.column
}

func (value *DefaultBytesValue) Value() interface{} {
	return value.value
}

func (value *DefaultBytesValue) BytesValue() []byte {
	return value.value
}

func (value *DefaultBytesValue) ToOldValue() (interface{}, error) {
	return value.value, nil
}
