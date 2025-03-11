package types

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type UInt32Value interface {
	base.Value
	UInt32Value() *uint32
}

type UInt32Type struct {
}

func NewUInt32Type() *UInt32Type {
	return &UInt32Type{}
}

func (typ *UInt32Type) Cast(value base.Value) (UInt32Value, error) {
	uint32Value, ok := value.(UInt32Value)
	if ok {
		return uint32Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to UInt32Value", value)
	}
}

func (typ *UInt32Type) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *UInt32Type) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeUint32, nil
}

type DefaultUInt32Value struct {
	column base.Column
	value  *uint32
}

func NewDefaultUInt32Value(value *uint32, column base.Column) *DefaultUInt32Value {
	return &DefaultUInt32Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultUInt32Value) Column() base.Column {
	return value.column
}

func (value *DefaultUInt32Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultUInt32Value) UInt32Value() *uint32 {
	return value.value
}

func (value *DefaultUInt32Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
