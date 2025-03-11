package types

import (
	"encoding/json"

	"github.com/transferria/transferria/pkg/base"
	"go.ytsaurus.tech/yt/go/schema"
)

type JSONValue interface {
	base.Value
	JSONValue() ([]byte, error)
}

type DefaultJSONValue struct {
	column base.Column
	raw    interface{}
}

func (v *DefaultJSONValue) Column() base.Column {
	return v.column
}

func (v *DefaultJSONValue) Value() interface{} {
	return v.raw
}

func (v *DefaultJSONValue) ToOldValue() (interface{}, error) {
	return v.raw, nil
}

func (v *DefaultJSONValue) JSONValue() ([]byte, error) {
	return json.Marshal(v.raw)
}

func NewDefaultJSONValue(value interface{}, column base.Column) *DefaultJSONValue {
	return &DefaultJSONValue{
		column: column,
		raw:    value,
	}
}

type JSONType struct{}

func (t *JSONType) Validate(value base.Value) error {
	return nil
}

func (t *JSONType) ToOldType() (schema.Type, error) {
	return schema.TypeAny, nil
}

func NewJSONType() *JSONType {
	return &JSONType{}
}
