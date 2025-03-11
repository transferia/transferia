package types

import (
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type IntervalValue interface {
	base.Value
	IntervalValue() *time.Duration
}

type IntervalType struct {
}

func NewIntervalType() *IntervalType {
	return &IntervalType{}
}

func (typ *IntervalType) Cast(value base.Value) (IntervalValue, error) {
	intervalValue, ok := value.(IntervalValue)
	if ok {
		return intervalValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to IntervalValue", value)
	}
}

func (typ *IntervalType) Validate(value base.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *IntervalType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeInterval, nil
}

type DefaultIntervalValue struct {
	column base.Column
	value  *time.Duration
}

func NewDefaultIntervalValue(value *time.Duration, column base.Column) *DefaultIntervalValue {
	return &DefaultIntervalValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultIntervalValue) Column() base.Column {
	return value.column
}

func (value *DefaultIntervalValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultIntervalValue) IntervalValue() *time.Duration {
	return value.value
}

func (value *DefaultIntervalValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
