//go:build !disable_mysql_provider

package replication

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/unmarshaller/types"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/util/strict"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/xerrors"
)

func UnmarshalHetero(value any, colSchema *abstract.ColSchema, location *time.Location) (any, error) {
	if value == nil {
		return nil, nil
	}

	var result any
	var err error

	targetType := schema.Type(colSchema.DataType)
	switch targetType {
	case schema.TypeInt64:
		result, err = strict.Expected[int64](value, cast.ToInt64E)
	case schema.TypeInt32:
		result, err = strict.Expected[int32](value, cast.ToInt32E)
	case schema.TypeInt16:
		result, err = strict.Expected[int16](value, cast.ToInt16E)
	case schema.TypeInt8:
		result, err = strict.Expected[int8](value, cast.ToInt8E)
	case schema.TypeUint64:
		result, err = strict.Expected[uint64](value, cast.ToUint64E)
	case schema.TypeUint32:
		result, err = strict.Expected[uint32](value, cast.ToUint32E)
	case schema.TypeUint16:
		result, err = strict.Expected[uint16](value, cast.ToUint16E)
	case schema.TypeUint8:
		result, err = strict.Expected[uint8](value, cast.ToUint8E)
	case schema.TypeFloat32:
		result, err = strict.Unexpected(value, cast.ToFloat32E)
	case schema.TypeFloat64:
		switch v := value.(type) {
		case decimal.Decimal:
			result, err = strict.Expected[decimal.Decimal](v, castx.ToJSONNumberE)
		case float32:
			result, err = strict.Expected[float32](v, castx.ToJSONNumberE)
		case float64:
			result, err = strict.Expected[float64](v, castx.ToJSONNumberE)
		default:
			result, err = strict.Unexpected(v, castx.ToJSONNumberE)
		}
	case schema.TypeBytes:
		switch v := value.(type) {
		case []byte:
			result, err = strict.Expected[[]byte](v, castx.ToByteSliceE)
		case int64:
			result, err = strict.Expected[int64](v, makeCastUint64ToBytes(colSchema.OriginalType))
		default:
			result, err = strict.Unexpected(v, castx.ToByteSliceE)
		}
	case schema.TypeBoolean:
		result, err = strict.Unexpected(value, cast.ToBoolE)
	case schema.TypeDate:
		castToTimeByTemporal := func(value any) (any, error) { return castToTimeByTemporalInLocation(value, time.UTC) }
		result, err = strict.Expected[string](value, castToTimeByTemporal)
	case schema.TypeDatetime:
		result, err = strict.Unexpected(value, cast.ToTimeE)
	case schema.TypeTimestamp:
		castToTimeByTemporal := func(value any) (any, error) { return castToTimeByTemporalInLocation(value, location) }
		result, err = strict.Expected[string](value, castToTimeByTemporal)
	case schema.TypeInterval:
		result, err = strict.Unexpected(value, cast.ToDurationE)
	case schema.TypeString:
		switch v := value.(type) {
		case string:
			result, err = strict.Expected[string](v, castx.ToStringE)
		case int: // representation of `year`
			result, err = strict.Expected[int](v, castx.ToStringE)
		default:
			result, err = strict.Unexpected(v, castx.ToStringE)
		}
	case schema.TypeAny:
		result, err = strict.Expected[string](value, castToAny)
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("unexpected target type %s (original type %q, value of type %T), unmarshalling is not implemented", colSchema.DataType, colSchema.OriginalType, value))
	}

	if err != nil {
		return nil, abstract.NewStrictifyError(colSchema, targetType, err)
	}
	return result, nil
}

func makeCastUint64ToBytes(originalType string) func(value any) (any, error) {
	return func(value any) (any, error) {
		buf := bytes.Buffer{}
		if err := binary.Write(&buf, binary.BigEndian, value); err != nil {
			return nil, xerrors.Errorf("failed to write into bytes.Buffer: %w", err)
		}
		result := buf.Bytes()
		if abstract.TrimMySQLType(originalType) != "bit" {
			return result, nil
		}
		// BIT(N)
		bitLength, err := typeLengthModifier(originalType)
		if err != nil {
			return nil, xerrors.Errorf("failed to get type modifier: %w", err)
		}
		if bitLength < 0 { // should never occur
			return result, nil
		}
		byteLength := int(math.Ceil(float64(bitLength) / 8.0))
		return result[8-byteLength:], nil
	}
}

// typeLengthModifier returns -1 if there is no length modifier for the type
func typeLengthModifier(originalType string) (int, error) {
	bracesOpenIdx := strings.Index(originalType, "(")
	if bracesOpenIdx < 0 {
		return -1, nil
	}
	bracesCloseIdx := strings.Index(originalType, ")")
	if bracesCloseIdx < 0 {
		return -1, nil
	}
	result, err := strconv.Atoi(originalType[bracesOpenIdx+1 : bracesCloseIdx])
	if err != nil {
		return -1, xerrors.Errorf("failed to parse length modifier: %w", err)
	}
	return result, nil
}

func castToTimeByTemporalInLocation(value any, location *time.Location) (any, error) {
	vBytes, err := castx.ToByteSliceE(value)
	if err != nil {
		return nil, xerrors.Errorf("failed to cast %T to []byte to convert it to temporal: %w", value, err)
	}

	temporal := types.NewTemporalInLocation(location)
	if err := temporal.Scan(vBytes); err != nil {
		return nil, xerrors.Errorf("failed to Scan temporal: %w", err)
	}
	result, err := temporal.Value()
	if err != nil {
		return nil, xerrors.Errorf("failed to get the Value of temporal: %w", err)
	}
	return result, nil
}

func castToAny(value any) (any, error) {
	vString, err := castx.ToStringE(value)
	if err != nil {
		return nil, xerrors.Errorf("failed to cast %T to string to convert it to any: %w", value, err)
	}

	// Special case for https://bugs.mysql.com/bug.php?id=87734
	// Replication client may receive empty string as JSON value if column is not nullable
	// but null has been inserted somehow
	if vString == "" {
		return jsonx.JSONNull{}, nil
	}

	decoder := jsonx.NewValueDecoder(jsonx.NewDefaultDecoder(strings.NewReader(vString)))
	result, err := decoder.Decode()
	if err != nil {
		return nil, xerrors.Errorf("failed to decode JSON: %w", err)
	}
	result, err = castx.ToJSONMarshallableE(result)
	if err != nil {
		return nil, xerrors.Errorf("the result of JSON decoding is not JSON-marshallable")
	}
	return result, nil
}
