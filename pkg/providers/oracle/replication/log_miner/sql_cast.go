//nolint:descriptiveerrors
package log_miner

import (
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/abstract2/types"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/util/xlocale"
)

func castToInt(column *oracle_schema.Column, valueStr *string, size int) (abstract2.Value, error) {
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
			return types.NewDefaultInt8Value(nil, column), nil
		}
		int8Value := int8(*nullableValue)
		return types.NewDefaultInt8Value(&int8Value, column), nil
	case 16:
		if nullableValue == nil {
			return types.NewDefaultInt16Value(nil, column), nil
		}
		int16Value := int16(*nullableValue)
		return types.NewDefaultInt16Value(&int16Value, column), nil
	case 32:
		if nullableValue == nil {
			return types.NewDefaultInt32Value(nil, column), nil
		}
		int32Value := int32(*nullableValue)
		return types.NewDefaultInt32Value(&int32Value, column), nil
	case 64:
		return types.NewDefaultInt64Value(nullableValue, column), nil
	}
	return nil, xerrors.Errorf("Invalid size of int '%v'", size)
}

func castToFloat(column *oracle_schema.Column, valueStr *string, size int) (abstract2.Value, error) {
	if size != 32 && size != 64 {
		return nil, xerrors.Errorf("Invalid size of float '%v'", size)
	}

	var nullableValue *float64
	if valueStr == nil || *valueStr == "NULL" {
		nullableValue = nil
	} else {
		value, err := strconv.ParseFloat(*valueStr, size)
		if err != nil {
			return nil, xerrors.Errorf("Can't parse value '%v' to float: %w", *valueStr, err)
		}
		nullableValue = &value
	}

	switch size {
	case 32:
		if nullableValue == nil {
			return types.NewDefaultFloatValue(nil, column), nil
		}
		value := float32(*nullableValue)
		return types.NewDefaultFloatValue(&value, column), nil
	case 64:
		return types.NewDefaultDoubleValue(nullableValue, column), nil
	}
	return nil, xerrors.Errorf("Invalid size of float '%v'", size)
}

func castToStringCore(valueStr *string) (*string, error) {
	// Input string looks like:
	//	   EMPTY_CLOB()
	//     'value'
	//	   value

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

	if strings.HasPrefix(*value, ".") { // Handle values like '.1'
		formatedValue := "0" + *value
		return &formatedValue, nil
	}

	return value, nil
}

func castToString(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	value, err := castToStringCore(valueStr)
	if err != nil {
		return nil, xerrors.Errorf("Error parse string value: %w", err)
	}

	return types.NewDefaultStringValue(value, column), nil
}

func castToDecimal(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	value, err := castToStringNumberCore(valueStr)
	if err != nil {
		return nil, err
	}

	return types.NewDefaultDecimalValue(value, column), nil
}

func castToBigFloat(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	value, err := castToStringNumberCore(valueStr)
	if err != nil {
		return nil, err
	}

	return types.NewDefaultBigFloatValue(value, column), nil
}

func castToBytes(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	// Input string looks like:
	//	   EMPTY_BLOB()
	//     HEXTORAW('12ab')

	if valueStr == nil || *valueStr == "NULL" {
		return types.NewDefaultBytesValue(nil, column), nil
	}

	if *valueStr == "EMPTY_BLOB()" {
		return types.NewDefaultBytesValue([]byte{}, column), nil
	}
	const hexPrefix = "HEXTORAW('"
	const hexSuffix = "')"
	if !strings.HasPrefix(*valueStr, hexPrefix) || !strings.HasSuffix(*valueStr, hexSuffix) {
		const maxChars = 30
		if len(*valueStr) > maxChars {
			return nil, xerrors.Errorf("Value '%v...' is not hex string", (*valueStr)[:maxChars]) // TODO: fix to utf8 substring
		} else {
			return nil, xerrors.Errorf("Value '%v' is not hex string", *valueStr)
		}
	}
	hexStr := (*valueStr)[len(hexPrefix) : len(*valueStr)-len(hexSuffix)]

	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, xerrors.Errorf("Can't decode hex string: %w", err)
	}
	return types.NewDefaultBytesValue(bytes, column), nil
}

func castToTimeCore(valueStr *string) (*time.Time, error) {
	// Input string looks like:
	//     TIMESTAMP ' 1997-01-31 00:00:00'
	//     TIMESTAMP ' 1997-01-31 09:26:56.660000'
	//     TIMESTAMP ' 1997-01-31 09:26:56.660000+02:00'
	//     TIMESTAMP ' 2021-09-17 18:19:09.728 Europe/Moscow MSK'

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
	// Try parse time
	timestamp, err := dateparse.ParseAny(parts[0] + " " + parts[1])
	if err != nil {
		return nil, xerrors.Errorf("Can't parse value '%v' to timestamp: %w", *valueStr, err)
	}
	// Try parse time location
	if len(parts) > 2 {
		var location *time.Location
		var err error
		for i := 2; i < len(parts); i++ {
			location, err = xlocale.Load(parts[i])
			if err != nil {
				continue
			}
			timestamp = time.Date(
				timestamp.Year(), timestamp.Month(), timestamp.Day(),
				timestamp.Hour(), timestamp.Minute(), timestamp.Second(),
				timestamp.Nanosecond(), location)
			break
		}
		if location == nil {
			return nil, xerrors.Errorf("Can't parse value '%v' to timestamp: %w", *valueStr, err)
		}
	}

	return &timestamp, nil
}

func castToDate(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	time, err := castToTimeCore(valueStr)
	if err != nil {
		return nil, err
	}
	// In Oracle 'date' is 'datetime' value
	return types.NewDefaultDateTimeValue(time, column), nil
}

func castToTimestamp(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	time, err := castToTimeCore(valueStr)
	if err != nil {
		return nil, err
	}
	return types.NewDefaultTimestampValue(time, column), nil
}

func castToTimestampTZ(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	time, err := castToTimeCore(valueStr)
	if err != nil {
		return nil, err
	}
	return types.NewDefaultTimestampTZValue(time, column), nil
}

func castToDSInterval(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	// Input string looks like: TO_DSINTERVAL('+00 00:15:30.000000')

	if valueStr == nil || *valueStr == "NULL" {
		return types.NewDefaultIntervalValue(nil, column), nil
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

	duration := time.Duration(interval)
	return types.NewDefaultIntervalValue(&duration, column), nil
}

func castValueFromLogMiner(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	value, err := castValueFromLogMinerCore(column, valueStr)
	if err != nil {
		return nil, xerrors.Errorf("Cast error, for column '%v', of type '%v(%v)': %w",
			column.OracleSQLName(), column.OracleType(), column.Type(), err)
	}
	return value, nil
}

//nolint:descriptiveerrors
func castValueFromLogMinerCore(column *oracle_schema.Column, valueStr *string) (abstract2.Value, error) {
	switch column.OracleBaseType() {
	case
		"ROWID", "UROWID",
		"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "LONG",
		"CLOB", "NCLOB":
		return castToString(column, valueStr)
	case "NUMBER":
		switch column.Type().(type) {
		case *types.Int8Type:
			return castToInt(column, valueStr, 8)
		case *types.Int16Type:
			return castToInt(column, valueStr, 16)
		case *types.Int32Type:
			return castToInt(column, valueStr, 32)
		case *types.Int64Type:
			return castToInt(column, valueStr, 64)
		case *types.FloatType:
			return castToFloat(column, valueStr, 32)
		case *types.DoubleType:
			return castToFloat(column, valueStr, 64)
		case *types.DecimalType:
			return castToDecimal(column, valueStr)
		default:
			return nil, xerrors.Errorf("Unsupported sub type '%v' for type '%v'. columnName: %v", column.OracleType(), column.Type(), column.FullName())
		}
	case "FLOAT":
		switch column.Type().(type) {
		case *types.FloatType:
			return castToFloat(column, valueStr, 32)
		case *types.DoubleType:
			return castToFloat(column, valueStr, 64)
		case *types.BigFloatType:
			return castToBigFloat(column, valueStr)
		default:
			return nil, xerrors.Errorf("Unsupported sub type '%v' for type '%v'. columnName: %v", column.OracleType(), column.Type(), column.FullName())
		}
	case "BINARY_FLOAT":
		return castToFloat(column, valueStr, 32)
	case "BINARY_DOUBLE":
		return castToFloat(column, valueStr, 64)
	case "RAW", "LONG RAW", "BLOB":
		return castToBytes(column, valueStr)
	case "DATE":
		return castToDate(column, valueStr)
	case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE":
		return castToTimestamp(column, valueStr)
	case "TIMESTAMP WITH TIME ZONE":
		return castToTimestampTZ(column, valueStr)
	case "INTERVAL DAY TO SECOND":
		return castToDSInterval(column, valueStr)
	default:
		return nil, xerrors.Errorf("Unsupported type '%v'", column.OracleType())
	}
}
