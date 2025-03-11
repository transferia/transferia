package types

import (
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type DateTimeValue interface {
	base.Value
	DateTimeValue() *time.Time
}

type DateTimeType struct {
}

func NewDateTimeType() *DateTimeType {
	return &DateTimeType{}
}

func (typ *DateTimeType) Cast(value base.Value) (DateTimeValue, error) {
	dateTimeValue, ok := value.(DateTimeValue)
	if ok {
		return dateTimeValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to DateTimeValue", value)
	}
}

func (typ *DateTimeType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *DateTimeType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeDatetime, nil
}

type DefaultDateTimeValue struct {
	column base.Column
	value  *time.Time
}

func NewDefaultDateTimeValue(value *time.Time, column base.Column) *DefaultDateTimeValue {
	return &DefaultDateTimeValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultDateTimeValue) Column() base.Column {
	return value.column
}

func (value *DefaultDateTimeValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultDateTimeValue) DateTimeValue() *time.Time {
	return value.value
}

func (value *DefaultDateTimeValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
