package castx

import (
	"fmt"
	"html/template"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cast"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/valyala/fastjson/fastfloat"
)

// ToByteSliceE casts the type to a slice of bytes. The returned slice is a copy of the original one.
func ToByteSliceE(v any) ([]byte, error) {
	switch vCasted := v.(type) {
	case []byte:
		result := make([]byte, len(vCasted))
		copy(result, vCasted)
		return result, nil
	case string:
		return []byte(vCasted), nil
	default:
		return nil, xerrors.Errorf("no known conversion from %T to []byte", vCasted)
	}
}

func ToJSONMarshallableE[T any](v T) (T, error) {
	if _, err := json.Marshal(v); err != nil {
		return v, xerrors.Errorf("%T is not a JSON marshallable type: %w", v, err)
	}
	return v, nil
}

func ToJSONNumberE(v any) (json.Number, error) {
	vStr, err := cast.ToStringE(v)
	if err != nil {
		return json.Number("0"), err
	}
	result := json.Number(vStr)
	if _, floatParseErr := fastfloat.Parse(vStr); floatParseErr == nil {
		return result, nil
	}
	if _, intParseErr := result.Int64(); intParseErr == nil {
		return result, nil
	}
	return json.Number("0"), xerrors.Errorf("%s is not a parsable JSON number", vStr)
}

func ToStringE(i interface{}) (string, error) {
	// Here original spf13/cast uses reflect to restore the base type (or nil) or the fmt.Stringer implementation.
	// We will skip this step to improve performance, but keep the functionality in the "default" case.
	switch s := i.(type) {
	case string:
		return s, nil
	case bool:
		return strconv.FormatBool(s), nil
	case float64:
		return strconv.FormatFloat(s, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(s), 'f', -1, 32), nil
	case int:
		return strconv.Itoa(s), nil
	case int64:
		return strconv.FormatInt(s, 10), nil
	case int32:
		return strconv.Itoa(int(s)), nil
	case int16:
		return strconv.FormatInt(int64(s), 10), nil
	case int8:
		return strconv.FormatInt(int64(s), 10), nil
	case uint:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint64:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(s), 10), nil
	case json.Number:
		return s.String(), nil
	case []byte:
		return string(s), nil
	case template.HTML:
		return string(s), nil
	case template.URL:
		return string(s), nil
	case template.JS:
		return string(s), nil
	case template.CSS:
		return string(s), nil
	case template.HTMLAttr:
		return string(s), nil
	case nil:
		return "", nil
	case fmt.Stringer:
		return s.String(), nil
	case error:
		return s.Error(), nil
	default:
		return cast.ToStringE(i)
	}
}

// DurationToIso8601 extracts Days, Hours, Minutes and Seconds from provided duration and casts to ISO 8601 standart.
// E.g. P750DT23H59M59S, more on https://en.wikipedia.org/wiki/ISO_8601#Durations.
func DurationToIso8601(duration time.Duration) (string, error) {
	if duration.Nanoseconds() < 0 {
		return "", xerrors.Errorf("unable to cast negative duration '%s' to ISO 8601", duration.String())
	}
	time := "T"
	if h := int(duration.Hours()) % 24; h > 0 {
		time += fmt.Sprint(h) + "H"
	}
	if m := int(duration.Minutes()) % 60; m > 0 {
		time += fmt.Sprint(m) + "M"
	}
	if s := int(duration.Seconds()) % 60; s > 0 {
		time += fmt.Sprint(s) + "S"
	}

	res := "P"
	if d := int(duration.Hours()) / 24; d > 0 {
		res += fmt.Sprint(d) + "D"
	}
	if time != "T" {
		res += time
	}
	if res == "P" {
		res = "P0D"
	}
	return res, nil
}
