//nolint:descriptiveerrors
package log_miner

import (
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/transferia/transferia/library/go/core/xerrors"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/util/xlocale"
)

func castToInt(valueStr *string, size int) (interface{}, error) {
	if size != 8 && size != 16 && size != 32 && size != 64 {
		return nil, xerrors.Errorf("Invalid size of int '%v'", size)
	}

	var nullableValue *int64
	if valueStr == nil || *valueStr == "NULL" {
		nullableValue = nil
	} else {
		value, err := strconv.ParseInt(*valueStr, 10, size)
		if err != nil {
			return nil, xerrors.Errorf("Can't parse value '%v' to int: %w", *valueStr, err)
		}
		nullableValue = &value
	}
	switch size {
	case 8:
		if nullableValue == nil {
			return nil, nil
		}
		return int8(*nullableValue), nil
	case 16:
		if nullableValue == nil {
			return nil, nil
		}
		return int16(*nullableValue), nil
	case 32:
		if nullableValue == nil {
			return nil, nil
		}
		return int32(*nullableValue), nil
	case 64:
		if nullableValue == nil {
			return nil, nil
		}
		return *nullableValue, nil
	}
	return nil, xerrors.Errorf("Invalid size of int '%v'", size)
}

func castToFloat(valueStr *string, size int) (interface{}, error) {
	if size != 32 && size != 64 {
		return nil, xerrors.Errorf("Invalid size of float '%v'", size)
	}

	var nullableValue *float64
	if valueStr == nil || *valueStr == "NULL" {
		return nil, nil
	}
	value, err := strconv.ParseFloat(*valueStr, size)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse value '%v' to float: %w", *valueStr, err)
	}
	nullableValue = &value

	switch size {
	case 32:
		return float32(*nullableValue), nil
	case 64:
		return *nullableValue, nil
	}
	return nil, xerrors.Errorf("Invalid size of float '%v'", size)
}

func castToStringCore(valueStr *string) (*string, error) {
	if valueStr == nil || *valueStr == "NULL" {
		return nil, nil
	}

	if *valueStr == "EMPTY_CLOB()" {
		empty := ""
		return &empty, nil
	}

	const strQuote = "'"
	trimmedValuesStr := *valueStr
	if strings.HasPrefix(trimmedValuesStr, strQuote) && strings.HasSuffix(trimmedValuesStr, strQuote) {
		strQuoteLen := len(strQuote)
		trimmedValuesStr = trimmedValuesStr[strQuoteLen : len(trimmedValuesStr)-strQuoteLen]
	}
	return &trimmedValuesStr, nil
}

func castToStringNumberCore(valueStr *string) (*string, error) {
	value, err := castToStringCore(valueStr)
	if err != nil {
		return nil, xerrors.Errorf("Error parse string value: %w", err)
	}

	if value == nil {
		return value, nil
	}

	if strings.HasPrefix(*value, ".") {
		formatedValue := "0" + *value
		return &formatedValue, nil
	}

	return value, nil
}

func castToStringValue(valueStr *string) (interface{}, error) {
	value, err := castToStringCore(valueStr)
	if err != nil {
		return nil, xerrors.Errorf("Error parse string value: %w", err)
	}
	if value == nil {
		return nil, nil
	}
	return *value, nil
}

func castToDecimalString(valueStr *string) (interface{}, error) {
	value, err := castToStringNumberCore(valueStr)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return *value, nil
}

func castToBytesValue(valueStr *string) (interface{}, error) {
	if valueStr == nil || *valueStr == "NULL" {
		return nil, nil
	}

	if *valueStr == "EMPTY_BLOB()" {
		return []byte{}, nil
	}
	const hexPrefix = "HEXTORAW('"
	const hexSuffix = "')"
	if !strings.HasPrefix(*valueStr, hexPrefix) || !strings.HasSuffix(*valueStr, hexSuffix) {
		const maxChars = 30
		if len(*valueStr) > maxChars {
			return nil, xerrors.Errorf("Value '%v...' is not hex string", (*valueStr)[:maxChars])
		}
		return nil, xerrors.Errorf("Value '%v' is not hex string", *valueStr)
	}
	hexStr := (*valueStr)[len(hexPrefix) : len(*valueStr)-len(hexSuffix)]

	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, xerrors.Errorf("Can't decode hex string: %w", err)
	}
	return b, nil
}

func castToTimeCore(valueStr *string) (*time.Time, error) {
	if valueStr == nil || *valueStr == "NULL" {
		return nil, nil
	}

	const timestampPrefix = "TIMESTAMP"
	if !strings.HasPrefix(*valueStr, timestampPrefix) {
		return nil, xerrors.Errorf("Value '%v' is not timestamp string", *valueStr)
	}
	timestampStr := (*valueStr)[len(timestampPrefix):]
	timestampStr = strings.TrimFunc(timestampStr, func(char rune) bool {
		return char == ' ' || char == '\''
	})

	parts := strings.Fields(timestampStr)
	ts, err := dateparse.ParseAny(parts[0] + " " + parts[1])
	if err != nil {
		return nil, xerrors.Errorf("Can't parse value '%v' to timestamp: %w", *valueStr, err)
	}
	if len(parts) > 2 {
		var location *time.Location
		var locErr error
		for i := 2; i < len(parts); i++ {
			location, locErr = xlocale.Load(parts[i])
			if locErr != nil {
				continue
			}
			ts = time.Date(
				ts.Year(), ts.Month(), ts.Day(),
				ts.Hour(), ts.Minute(), ts.Second(),
				ts.Nanosecond(), location)
			break
		}
		if location == nil {
			return nil, xerrors.Errorf("Can't parse value '%v' to timestamp: %w", *valueStr, locErr)
		}
	}

	return &ts, nil
}

func castToDateValue(valueStr *string) (interface{}, error) {
	t, err := castToTimeCore(valueStr)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, nil
	}
	return *t, nil
}

func castToTimestampValue(valueStr *string) (interface{}, error) {
	return castToDateValue(valueStr)
}

func castToTimestampTZValue(valueStr *string) (interface{}, error) {
	return castToDateValue(valueStr)
}

func castToYMIntervalStringValue(valueStr *string) (interface{}, error) {
	if valueStr == nil || *valueStr == "NULL" {
		return nil, nil
	}
	const prefix = "TO_YMINTERVAL('"
	const suffix = "')"
	if !strings.HasPrefix(*valueStr, prefix) || !strings.HasSuffix(*valueStr, suffix) {
		return nil, xerrors.Errorf("Value '%v' is not a year to month interval string", *valueStr)
	}
	return (*valueStr)[len(prefix) : len(*valueStr)-len(suffix)], nil
}

func castToDSIntervalValue(valueStr *string) (interface{}, error) {
	if valueStr == nil || *valueStr == "NULL" {
		return nil, nil
	}

	const intervalPrefix = "TO_DSINTERVAL('"
	const intervalSuffix = "')"
	if !strings.HasPrefix(*valueStr, intervalPrefix) || !strings.HasSuffix(*valueStr, intervalSuffix) {
		return nil, xerrors.Errorf("Value '%v' is not day to second interval string", *valueStr)
	}
	intervalStr := (*valueStr)[len(intervalPrefix) : len(*valueStr)-len(intervalSuffix)]

	parts := strings.FieldsFunc(intervalStr, func(char rune) bool {
		return char == '+' || char == '-' || char == ' ' || char == ':' || char == '.'
	})
	if len(parts) != 5 {
		return nil, xerrors.Errorf("Can't parse value '%v' to day to second interval", *valueStr)
	}

	interval := int64(0)

	days, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse value '%v' to day to second interval: %w", *valueStr, err)
	}
	interval += days * 24 * int64(time.Hour)

	hours, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse value '%v' to day to second interval: %w", *valueStr, err)
	}
	interval += hours * int64(time.Hour)

	minutes, err := strconv.ParseInt(parts[2], 10, 32)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse value '%v' to day to second interval: %w", *valueStr, err)
	}
	interval += minutes * int64(time.Minute)

	seconds, err := strconv.ParseInt(parts[3], 10, 32)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse value '%v' to day to second interval: %w", *valueStr, err)
	}
	interval += seconds * int64(time.Second)

	milliseconds, err := strconv.ParseInt(parts[4], 10, 32)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse value '%v' to day to second interval: %w", *valueStr, err)
	}
	interval += milliseconds * int64(time.Millisecond)

	if strings.HasPrefix(intervalStr, "-") {
		interval *= -1
	}

	return time.Duration(interval), nil
}

func castValueFromLogMiner(column *oracle_schema.Column, valueStr *string) (interface{}, error) {
	value, err := castValueFromLogMinerCore(column, valueStr)
	if err != nil {
		return nil, xerrors.Errorf("Cast error, for column '%v', of type '%v': %w",
			column.OracleSQLName(), column.OracleType(), err)
	}
	return value, nil
}

//nolint:descriptiveerrors
func castValueFromLogMinerCore(column *oracle_schema.Column, valueStr *string) (interface{}, error) {
	switch column.OracleBaseType() {
	case
		"ROWID", "UROWID",
		"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "LONG",
		"CLOB", "NCLOB":
		return castToStringValue(valueStr)
	case "NUMBER":
		dt := column.DataType()
		switch dt {
		case oracle_schema.YTTypeInt8:
			return castToInt(valueStr, 8)
		case oracle_schema.YTTypeInt16:
			return castToInt(valueStr, 16)
		case oracle_schema.YTTypeInt32:
			return castToInt(valueStr, 32)
		case oracle_schema.YTTypeInt64:
			return castToInt(valueStr, 64)
		case oracle_schema.YTTypeFloat32:
			return castToFloat(valueStr, 32)
		case oracle_schema.YTTypeFloat64:
			return castToFloat(valueStr, 64)
		case oracle_schema.YTTypeString:
			return castToDecimalString(valueStr)
		default:
			return nil, xerrors.Errorf("Unsupported data type '%v' for NUMBER. columnName: %v", dt, column.FullName())
		}
	case "FLOAT":
		switch column.FloatBitSize() {
		case 32:
			return castToFloat(valueStr, 32)
		case 64:
			return castToFloat(valueStr, 64)
		default:
			return castToDecimalString(valueStr)
		}
	case "BINARY_FLOAT":
		return castToFloat(valueStr, 32)
	case "BINARY_DOUBLE":
		return castToFloat(valueStr, 64)
	case "RAW", "LONG RAW", "BLOB":
		return castToBytesValue(valueStr)
	case "DATE":
		return castToDateValue(valueStr)
	case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE":
		return castToTimestampValue(valueStr)
	case "TIMESTAMP WITH TIME ZONE":
		return castToTimestampTZValue(valueStr)
	case "INTERVAL YEAR TO MONTH":
		return castToYMIntervalStringValue(valueStr)
	case "INTERVAL DAY TO SECOND":
		return castToDSIntervalValue(valueStr)
	default:
		return nil, xerrors.Errorf("Unsupported type '%v'", column.OracleType())
	}
}
