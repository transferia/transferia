package types

import (
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type DateValue interface {
	base.Value
	DateValue() *time.Time
}

type DateType struct {
}

func NewDateType() *DateType {
	return &DateType{}
}

func (typ *DateType) Cast(value base.Value) (DateValue, error) {
	dateValue, ok := value.(DateValue)
	if ok {
		return dateValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to DateValue", value)
	}
}

func (typ *DateType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *DateType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeDate, nil
}

type DefaultDateValue struct {
	column base.Column
	value  *time.Time
}

func NewDefaultDateValue(value *time.Time, column base.Column) *DefaultDateValue {
	return &DefaultDateValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultDateValue) Column() base.Column {
	return value.column
}

func (value *DefaultDateValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultDateValue) DateValue() *time.Time {
	return value.value
}

func (value *DefaultDateValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}

	return *value.value, nil
}
