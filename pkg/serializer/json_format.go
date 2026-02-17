package serializer

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

// toJsonValue converts a value to its JSON representation based on the schema type.
// The returned value is then passed to json.Encoder which performs the final serialization.
// When colSchema is provided, the conversion is type-aware.
// When colSchema is nil, the value is passed through to json.Encoder as-is.
//
// Type mapping (when schema is available):
//
//	Schema Type              | Go Type (after strictify) | JSON Output
//	-------------------------|---------------------------|------------------------------------------
//	int8..int64              | int8..int64               | number: 42
//	uint8..uint64            | uint8..uint64             | number: 100
//	float32                  | float32                   | number: 1.5
//	float64                  | json.Number               | number: 3.14 (json.Number implements json.Marshaler)
//	boolean                  | bool                      | boolean: true / false
//	string                   | string                    | string: "hello"
//	bytes                    | []byte                    | base64 string: "aGVsbG8=" (Go json encodes []byte as base64)
//	date/datetime/timestamp  | time.Time                 | RFC3339Nano string: "2024-01-15T12:30:45.123456789Z"
//	interval                 | time.Duration             | integer nanoseconds: 5000000000 (time.Duration is int64)
//	any (AnyAsString=false)  | any                       | native JSON: {"key":"value"} / [1,2,3]
//	any (AnyAsString=true)   | any                       | JSON string: "{\"key\":\"value\"}"
//	nil                      | nil                       | null
func toJsonValue(value any, colSchema *abstract.ColSchema, anyAsString bool) (any, error) {
	if value == nil {
		return nil, nil // → JSON null
	}

	if colSchema == nil {
		return nil, xerrors.Errorf("column schema is nil")
	}

	switch schema.Type(colSchema.DataType) {
	case schema.TypeInt8, schema.TypeInt16, schema.TypeInt32, schema.TypeInt64:
		return value, nil // → JSON number via json.Encoder

	case schema.TypeUint8, schema.TypeUint16, schema.TypeUint32, schema.TypeUint64:
		return value, nil // → JSON number via json.Encoder

	case schema.TypeFloat32:
		return value, nil // → JSON number via json.Encoder

	case schema.TypeFloat64:
		return value, nil // → JSON number (json.Number implements json.Marshaler)

	case schema.TypeBoolean:
		return value, nil // → JSON boolean via json.Encoder

	case schema.TypeString:
		return value, nil // → JSON string via json.Encoder

	case schema.TypeBytes:
		return value, nil // → base64 JSON string (Go json.Encoder encodes []byte as base64)

	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return value, nil // → RFC3339 JSON string (time.Time implements json.Marshaler)

	case schema.TypeInterval:
		return value, nil // → JSON integer nanoseconds (time.Duration is int64)

	case schema.TypeAny:
		if anyAsString {
			valueData, err := json.Marshal(value)
			if err != nil {
				return nil, xerrors.Errorf("toJsonValue: failed to marshal TypeAny value to string: %w", err)
			}
			return string(valueData), nil
		}
		return value, nil // → native JSON value via json.Encoder

	default:
		return value, nil // → pass through to json.Encoder
	}
}
