package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type UInt64Value interface {
	base.Value
	UInt64Value() *uint64
}

type UInt64Type struct {
}

func NewUInt64Type() *UInt64Type {
	return &UInt64Type{}
}

func (typ *UInt64Type) Cast(value base.Value) (UInt64Value, error) {
	uint64Value, ok := value.(UInt64Value)
	if ok {
		return uint64Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to UInt64Value", value)
	}
}

func (typ *UInt64Type) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *UInt64Type) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeUint64, nil
}

type DefaultUInt64Value struct {
	column base.Column
	value  *uint64
}

func NewDefaultUInt64Value(value *uint64, column base.Column) *DefaultUInt64Value {
	return &DefaultUInt64Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultUInt64Value) Column() base.Column {
	return value.column
}

func (value *DefaultUInt64Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultUInt64Value) UInt64Value() *uint64 {
	return value.value
}

func (value *DefaultUInt64Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
