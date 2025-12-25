package pg2ch

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/yt/go/schema"
)

// PG2CHDataTypesComparator properly compares data types in pg2ch transfers, skipping special cases.
func PG2CHDataTypesComparator(l, r string) bool {
	// ClickHouse converts any to string
	if l == schema.TypeAny.String() && r == "string" {
		return true
	}
	// ClickHouse converts utf8 to string
	if l == schema.TypeString.String() && r == "string" {
		return true
	}
	if l == schema.TypeBoolean.String() && r == schema.TypeUint8.String() {
		return true
	}
	return l == r
}

// PG2CHDataTypesComparatorV1 properly compares data types in pg2ch transfers, skipping special cases.
//
// This is a version of this comparator for type system `1`.
func PG2CHDataTypesComparatorV1(l, r string) bool {
	// ClickHouse converts YT DateTime and Timestamp to utf8
	if l == schema.TypeTimestamp.String() || l == schema.TypeDatetime.String() || l == schema.TypeDate.String() {
		return r == "string"
	}
	return PG2CHDataTypesComparator(l, r)
}

func ValueComparator(l interface{}, lCol abstract.ColSchema, r interface{}, rCol abstract.ColSchema, _ bool) (comparable bool, result bool, err error) {
	if boolV, ok := l.(bool); ok {
		return true, boolV == (r == uint8(1)), nil
	}
	if lfloat64V, ok := l.(float64); ok {
		if rfloat64V, ok := r.(float64); ok {
			// TODO: Fix and Remove
			// looks like CH looze some data
			// (source) 2.802596928649634e-45 [float64 | "pg:real" | "real"] != 2.8026e-45 [float64 | "ch:Nullable(Float64)" | ""]
			return true, fmt.Sprintf("%.8f", lfloat64V) == fmt.Sprintf("%.8f", rfloat64V), nil
		}
	}
	if lCol.OriginalType == "pg:time without time zone" {
		// ch drop rest of zeros
		lString := l.(string)
		rString := r.(string)
		lString = strings.ReplaceAll(lString, ".000000", "")
		return true, strings.TrimRightFunc(lString, func(r rune) bool {
			return r == '0'
		}) == rString, nil
	}
	if lCol.OriginalType == "pg:interval" {
		// ch drop tail of .00000
		lString := l.(string)
		rString := r.(string)

		return true, strings.ReplaceAll(lString, ".000000", "") == rString, nil
	}
	return false, false, nil
}

func ValueJSONNullComparator(l interface{}, lCol abstract.ColSchema, r interface{}, rCol abstract.ColSchema, _ bool) (comparable bool, result bool, err error) {
	_, lIsJSONNull := l.(jsonx.JSONNull)
	if !lIsJSONNull {
		return false, false, nil
	}

	if r == nil {
		return true, true, nil
	}
	if rString, rStringCasts := r.(string); rStringCasts {
		return true, rString == "null", nil
	}
	return true, false, nil
}
