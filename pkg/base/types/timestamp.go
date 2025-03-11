package types

import (
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type TimestampValue interface {
	base.Value
	TimestampValue() *time.Time
}

type TimestampType struct {
	precision int
}

func NewTimestampType(precision int) *TimestampType {
	return &TimestampType{
		precision: precision,
	}
}

func (typ *TimestampType) Cast(value base.Value) (TimestampValue, error) {
	dateValue, ok := value.(TimestampValue)
	if ok {
		return dateValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to TimestampValue", value)
	}
}

func (typ *TimestampType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *TimestampType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeTimestamp, nil
}

func (typ *TimestampType) Precision() int {
	return typ.precision
}

type DefaultTimestampValue struct {
	column base.Column
	value  *time.Time
}

func NewDefaultTimestampValue(value *time.Time, column base.Column) *DefaultTimestampValue {
	return &DefaultTimestampValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultTimestampValue) Column() base.Column {
	return value.column
}

func (value *DefaultTimestampValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultTimestampValue) TimestampValue() *time.Time {
	return value.value
}

func (value *DefaultTimestampValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
