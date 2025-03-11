package filterrows

import (
	"fmt"
	"math"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/library/go/yandex/cloud/filter"
	"github.com/transferria/transferria/pkg/util/set"
)

var errIntOverflow = xerrors.Errorf("Provided value overflows int64")

func stringToTime(str string) (time.Time, bool) {
	layouts := []string{
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000-0700",
		time.Layout,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		time.RFC822,
		time.RFC822Z,
		time.RFC850,
		time.RFC1123,
		time.RFC1123Z,
		time.RFC3339,
		time.RFC3339Nano,
		time.Kitchen,
		time.Stamp,
		time.StampMilli,
		time.StampMicro,
		time.StampNano,
		time.DateTime,
		time.DateOnly,
		time.TimeOnly,
	}
	for _, layout := range layouts {
		if res, err := time.Parse(layout, str); err == nil {
			return res, true
		}
	}
	return time.Time{}, false
}

// toInt64E casts an interface with any type of int/uint to an int64 type.
func toInt64E(i interface{}) (int64, error) {
	// We cannot use ToInt64E from https://github.com/spf13/cast/blob/master/caste.go
	// because it can convert float to int by just rounding it down.
	switch v := i.(type) {
	case int:
		return int64(v), nil
	case int64:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, errIntOverflow
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("unable to cast %#v of type %T to int64", i, i)
	}
}

func valuesListToSet(valList filter.Value) (*set.Set[interface{}], error) {
	values := make([]interface{}, 0)
	if valList.IsIntList() {
		for _, val := range valList.AsIntList() {
			values = append(values, val)
		}
	}

	if valList.IsFloatList() {
		for _, val := range valList.AsFloatList() {
			values = append(values, val)
		}
	}

	if valList.IsStringList() {
		for _, val := range valList.AsStringList() {
			values = append(values, val)
		}
	}

	if valList.IsTimeList() {
		for _, val := range valList.AsTimeList() {
			values = append(values, val.UnixMicro())
		}
	}

	if len(values) > 0 {
		return set.New(values...), nil
	}

	return nil, xerrors.Errorf("not appropriate type of list values: %s", valList.Type())
}
