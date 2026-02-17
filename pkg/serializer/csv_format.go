package serializer

import (
	"encoding/base64"
	"encoding/json"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/castx"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/xerrors"
)

// toCsvValue converts a value to its CSV string representation based on the schema type.
// When colSchema is provided, the conversion is type-aware and produces a well-defined output.
// When colSchema is nil, falls back to generic string conversion via castx.ToStringE.
//
// Type mapping (when schema is available):
//
//	Schema Type              | Go Type (after strictify) | CSV Output
//	-------------------------|---------------------------|------------------------------------------
//	int8..int64              | intN                      | decimal string: "42"
//	uint8..uint64            | uintN                     | decimal string: "100"
//	float32                  | float32                   | decimal string: "1.5"
//	float64                  | json.Number               | number string: "3.14"
//	boolean                  | bool                      | "true" or "false"
//	string                   | string                    | as-is: "hello"
//	bytes                    | []byte                    | base64 string: "aGVsbG8="
//	date/datetime/timestamp  | time.Time                 | RFC3339Nano: "2024-01-15T12:30:45.123456789Z"
//	interval                 | time.Duration             | Go duration string: "5s", "1h30m"
//	any                      | any                       | JSON-marshaled: "{\"key\":\"value\"}"
//	nil                      | nil                       | empty string: ""
func toCsvValue(value any, colSchema *abstract.ColSchema) (string, error) {
	if value == nil {
		return "", nil
	}

	if colSchema == nil {
		return "", xerrors.Errorf("column schema is nil")
	}

	var err error
	var csvValue string
	switch schema.Type(colSchema.DataType) {
	case schema.TypeBoolean:
		// Boolean is stored as bool after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeInt8:
		// Int8 is stored as int8 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeInt16:
		// Int16 is stored as int16 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeInt32:
		// Int32 is stored as int32 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeInt64:
		// Int64 is stored as int64 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeUint8:
		// Uint8 is stored as uint8 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeUint16:
		// Uint16 is stored as uint16 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeUint32:
		// Uint32 is stored as uint32 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeUint64:
		// Uint64 is stored as uint64 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeFloat32:
		// Float32 is stored as float32 after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeFloat64:
		// Float64 is stored as json.Number after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeString:
		// String is stored as string after strictify
		csvValue, err = castx.ToStringE(value)

	case schema.TypeBytes:
		// Bytes is stored as []byte after strictify
		// Encode bytes as base64 to safely represent arbitrary binary data in CSV.
		// Direct string([]byte) conversion can corrupt data with invalid UTF-8 sequences.
		b, ok := value.([]byte)
		if !ok {
			return "", xerrors.Errorf("value is not []byte")
		}
		return base64.StdEncoding.EncodeToString(b), nil

	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		// After strictify, value is always time.Time.
		csvValue, err = castx.ToStringE(value)

	case schema.TypeInterval:
		// After strictify, value is always time.Duration.
		csvValue, err = castx.ToStringE(value)

	case schema.TypeAny:
		// Any is json.Marshalable after strictify
		rawJSON, err := json.Marshal(value)
		if err != nil {
			return "", xerrors.Errorf("unable to marshal TypeAny value to JSON: %w", err)
		}
		return string(rawJSON), nil

	default:
		// Unknown type is stored as string after strictify
		csvValue, err = toCsvValueUntyped(value)
	}
	if err != nil {
		return "", xerrors.Errorf("unable to convert value of column type %s to CSV value: %w", colSchema.DataType, err)
	}

	return csvValue, err
}

// toCsvValueUntyped converts a value to CSV string without schema information.
// Uses castx.ToStringE with JSON marshal fallback for complex types.
func toCsvValueUntyped(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	cell, err := castx.ToStringE(value)
	if err != nil {
		rawJSON, err := json.Marshal(value)
		if err != nil {
			return "", xerrors.Errorf("unable to marshal value to JSON: %w", err)
		}
		return string(rawJSON), nil
	}
	return cell, nil
}
