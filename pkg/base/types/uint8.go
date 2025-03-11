package types

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type UInt8Value interface {
	base.Value
	UInt8Value() *uint8
}

type UInt8Type struct {
}

func NewUInt8Type() *UInt8Type {
	return &UInt8Type{}
}

func (typ *UInt8Type) Cast(value base.Value) (UInt8Value, error) {
	uint8Value, ok := value.(UInt8Value)
	if ok {
		return uint8Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to UInt8Value", value)
	}
}

func (typ *UInt8Type) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *UInt8Type) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeUint8, nil
}

type DefaultUInt8Value struct {
	column base.Column
	value  *uint8
}

func NewDefaultUInt8Value(value *uint8, column base.Column) *DefaultUInt8Value {
	return &DefaultUInt8Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultUInt8Value) Column() base.Column {
	return value.column
}

func (value *DefaultUInt8Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultUInt8Value) UInt8Value() *uint8 {
	return value.value
}

func (value *DefaultUInt8Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
