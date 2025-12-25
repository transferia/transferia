package types

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type TimestampTZValue interface {
	base.Value
	TimestampTZValue() *time.Time
}

type TimestampTZType struct {
	precision int
}

func NewTimestampTZType(precision int) *TimestampTZType {
	return &TimestampTZType{
		precision: precision,
	}
}

func (typ *TimestampTZType) Cast(value base.Value) (TimestampTZValue, error) {
	dateValue, ok := value.(TimestampTZValue)
	if ok {
		return dateValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to TimestampTZValue", value)
	}
}

func (typ *TimestampTZType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *TimestampTZType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeTimestamp, nil
}

func (typ *TimestampTZType) Precision() int {
	return typ.precision
}

type DefaultTimestampTZValue struct {
	column base.Column
	value  *time.Time
}

func NewDefaultTimestampTZValue(value *time.Time, column base.Column) *DefaultTimestampTZValue {
	return &DefaultTimestampTZValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultTimestampTZValue) Column() base.Column {
	return value.column
}

func (value *DefaultTimestampTZValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultTimestampTZValue) TimestampTZValue() *time.Time {
	return value.value
}

func (value *DefaultTimestampTZValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
