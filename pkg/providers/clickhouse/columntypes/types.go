//go:build !disable_clickhouse_provider

package columntypes

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	maxDate = time.Date(2106, time.January, 1, 0, 0, 0, 0, time.UTC)
	minDate = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
)

func applyClickhouseDateBoundaries(v time.Time) interface{} {
	// TODO: take care about Date32/DateTime64
	if v.Sub(maxDate) > 0 {
		return maxDate
	}
	if v.Sub(minDate) < 0 {
		return minDate
	}
	return v
}

func restoreMultiple(val interface{}, typePrefix string) interface{} {
	s := reflect.ValueOf(val)
	if s.Kind() != reflect.Slice {
		return marshalAny(val)
	}

	vals := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		vals[i] = s.Index(i).Interface()
	}

	return fmt.Sprintf("%v(%v)", typePrefix, joinSlice(vals))
}

func restoreTuple(val interface{}) interface{} {
	return restoreMultiple(val, "tuple")
}

func joinSlice(vals []interface{}) string {
	res := ""
	for i, val := range vals {
		if i > 0 {
			res += ", "
		}
		if s, ok := val.(string); ok {
			res = fmt.Sprintf("%v'%v'", res, s)
		} else {
			res = fmt.Sprintf("%v%v", res, val)
		}
	}
	return res
}

func marshalAny(val interface{}) interface{} {
	b, _ := json.Marshal(val)
	raw := string(b)
	if len(raw) > 0 {
		return raw
	}

	return ""
}

func Restore(column abstract.ColSchema, val interface{}) interface{} {
	switch strings.ToLower(column.DataType) {
	case "any":
		if v, ok := val.(string); ok {
			return v
		}
		if val == nil {
			return nil
		}

		switch {
		case strings.HasPrefix(column.OriginalType, "ch:Array"):
			return val
		case strings.HasPrefix(column.OriginalType, "ch:Tuple"):
			return restoreTuple(val)
		default:
			return marshalAny(val)
		}
	case schema.TypeDatetime.String(), schema.TypeDate.String():
		switch v := val.(type) {
		case time.Time:
			return applyClickhouseDateBoundaries(v)
		case *time.Time:
			if v == nil {
				return v
			}
			return applyClickhouseDateBoundaries(*v)
		case float64:
			return time.Unix(int64(v), 0)
		}
	}

	result := abstract.Restore(column, val)

	switch t := result.(type) {
	case json.Number:
		floatNum, _ := t.Float64()
		return floatNum
	default:
		return t
	}
}

// BaseType strips modifiers from the given ClickHouse type, if possible
func BaseType(chType string) string {
	extType, intType := divideTypeToExtAndInt(chType)
	if extType == "Nullable" {
		chType = intType
	}

	extType, intType = divideTypeToExtAndInt(chType)
	if extType == "LowCardinality" {
		chType = intType
	}

	extType, _ = divideTypeToExtAndInt(chType)

	return extType
}

// divideTypeToExtAndInt divides the given type into the "external" and "internal" parts. If there is no internal part, this method returns the original type.
func divideTypeToExtAndInt(chType string) (extPart string, intPart string) {
	matchesRaw := chTypeWithModifierRe.FindAllStringSubmatch(chType, 2)
	if len(matchesRaw) > 0 {
		match := matchesRaw[0]
		if len(match) != 3 {
			panic(fmt.Sprintf("expression parsed '%s' does not contain three parts; actual parts: %v", chType, match))
		}
		return match[1], match[2]
	}
	return chType, ""
}

var chTypeWithModifierRe *regexp.Regexp = regexp.MustCompile(`(\w*)\((.*)\)`)

// ToYtType converts the given ClickHouse type to YT type and a requiredness flag.
//
// XXX: support all types from system.data_type_families, such as Decimal, Tuple, Array, Nested, Array(Array(...))
func ToYtType(chType string) (ytType string, required bool) {
	var result schema.Type
	switch BaseType(chType) {
	case "Date":
		result = schema.TypeDate
	case "DateTime":
		result = schema.TypeDatetime
	case "DateTime64":
		result = schema.TypeTimestamp
	case "Enum8", "Enum16":
		result = schema.TypeString
	case "FixedString":
		result = schema.TypeBytes
	case "Float64":
		result = schema.TypeFloat64
	case "Int8":
		result = schema.TypeInt8
	case "Int16":
		result = schema.TypeInt16
	case "Int32":
		result = schema.TypeInt32
	case "Int64":
		result = schema.TypeInt64
	case "IPv4", "IPv6":
		result = schema.TypeString
	case "String":
		result = schema.TypeBytes
	case "UInt8":
		result = schema.TypeUint8
	case "UInt16":
		result = schema.TypeUint16
	case "UInt32":
		result = schema.TypeUint32
	case "UInt64":
		result = schema.TypeUint64
	default:
		result = schema.TypeAny
	}
	return string(result), !isNullable(chType)
}

func isNullable(chType string) bool {
	extType, _ := divideTypeToExtAndInt(chType)
	return extType == "Nullable"
}

func ToChType(ytType string) string {
	switch schema.Type(ytType) {
	case schema.TypeAny, schema.TypeBytes:
		return "String"
	case schema.TypeFloat64:
		return "Float64"
	case schema.TypeFloat32:
		return "Float32"
	case schema.TypeBoolean:
		return "UInt8"
	case schema.TypeInt8:
		return "Int8"
	case schema.TypeInt16:
		return "Int16"
	case schema.TypeInt32:
		return "Int32"
	case schema.TypeInt64:
		return "Int64"
	case schema.TypeUint8:
		return "UInt8"
	case schema.TypeUint16:
		return "UInt16"
	case schema.TypeUint32:
		return "UInt32"
	case schema.TypeUint64:
		return "UInt64"
	case schema.TypeDate:
		return "Date"
	case schema.TypeDatetime:
		return "DateTime"
	case schema.TypeTimestamp:
		return "DateTime64(6)"
	case schema.TypeInterval:
		return "Int64"
	default:
		return "String"
	}
}
