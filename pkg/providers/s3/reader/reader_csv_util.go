//go:build !disable_s3_provider

package reader

import (
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

// getCorrespondingValue performs a check/transformation with the original value against the user provided configuration
// such as: NullValues, TrueValues, FalseValues, TimestampParsers, DecimalPoint, to derive its corresponding value.
func (r *CSVReader) getCorrespondingValue(originalValue string, col abstract.ColSchema) interface{} {
	var resultingValue interface{}

	switch col.DataType {
	case schema.TypeBoolean.String():
		resultingValue = r.parseBooleanValue(originalValue)
	case schema.TypeDate.String(), schema.TypeDatetime.String():
		resultingValue = r.parseDateValue(originalValue)
	case schema.TypeTimestamp.String():
		resultingValue = r.parseTimestampValue(originalValue)
	case schema.TypeFloat32.String(), schema.TypeFloat64.String():
		resultingValue = r.parseFloatValue(originalValue)
	default:
		resultingValue = r.parseNullValues(originalValue, col)
	}

	return resultingValue
}

// parseFloatValue checks if the provided value can be correctly parsed to a float value
// if the specified decimal char is used.
// It defaults to the value itself if this converison is not possible.
func (r *CSVReader) parseFloatValue(originalValue string) interface{} {
	if r.additionalReaderOptions.DecimalPoint != "" {
		possibleFloat := strings.Replace(originalValue, r.additionalReaderOptions.DecimalPoint, ".", 1)
		_, err := strconv.ParseFloat(possibleFloat, 64)
		if err == nil {
			return possibleFloat
		} else {
			return originalValue
		}

	} else {
		return originalValue
	}
}

// parseNullValues checks if the provided value is part of the null values list provided by the user.
// If this is the case the zero value of the datatype is returned for this value.
// It defaults to the original value if the value is not contained in the list or if the  conditions of the
// boolean flags (QuotedStringsCanBeNull, StringsCanBeNull) are not fulfilled.
func (r *CSVReader) parseNullValues(originalValue string, col abstract.ColSchema) interface{} {
	if r.additionalReaderOptions.QuotedStringsCanBeNull {
		trimmedContent := originalValue
		if strings.HasPrefix(originalValue, "\"") && strings.HasSuffix(originalValue, "\"") {
			trimmedContent = strings.TrimSuffix(strings.TrimPrefix(originalValue, "\""), "\"")
		} else if strings.HasPrefix(originalValue, "'") && strings.HasSuffix(originalValue, "'") {
			trimmedContent = strings.TrimSuffix(strings.TrimPrefix(originalValue, "'"), "'")
		}
		if contains(r.additionalReaderOptions.NullValues, trimmedContent) {
			return abstract.DefaultValue(&col)
		}
	} else {
		if r.additionalReaderOptions.StringsCanBeNull {
			if contains(r.additionalReaderOptions.NullValues, originalValue) {
				return abstract.DefaultValue(&col)
			}
		}
	}

	return originalValue
}

// parseDateValue checks if the provided value can be parsed to a time.Time through one of
// the user provided TimestampParsers. It defaults to the original value if this is not the case.
func (r *CSVReader) parseDateValue(originalValue string) interface{} {
	for _, parser := range r.additionalReaderOptions.TimestampParsers {
		dateValue, err := time.Parse(parser, originalValue)
		if err == nil {
			return dateValue
		}
	}
	return originalValue
}

// parseTimestampValue checks if the provided value can be parsed to a time.Time.
// It defaults to the original value if this is not the case.
func (r *CSVReader) parseTimestampValue(originalValue string) interface{} {
	toInt64, err := strconv.ParseInt(originalValue, 10, 64)
	if err == nil {
		return time.Unix(toInt64, 0)
	}

	return originalValue
}

// parseBooleanValue checks if the provided value is contained in one of the provided lists of
// true/false values and returns the corresponding boolean value. If the value is contained in the null values
// then a false boolean value is returned for this value. It defaults to the original value if no matches are found.
func (r *CSVReader) parseBooleanValue(originalValue string) interface{} {
	if r.additionalReaderOptions.StringsCanBeNull {
		if contains(r.additionalReaderOptions.NullValues, originalValue) {
			return false
		}
	}
	if contains(r.additionalReaderOptions.TrueValues, originalValue) {
		return true
	} else if contains(r.additionalReaderOptions.FalseValues, originalValue) {
		return false
	} else {
		// last ditch attempt, try string conversion
		boolVal, err := strconv.ParseBool(originalValue)
		if err != nil {
			return originalValue
		}
		return boolVal
	}
}
