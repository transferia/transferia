package snapshot

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
)

//nolint:descriptiveerrors
func createRawValue(column *oracle_schema.Column) (interface{}, error) {
	switch column.OracleBaseType() {
	case
		"ROWID", "UROWID",
		"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "LONG",
		"JSON":
		return new(string), nil
	case "CLOB", "NCLOB":
		return createCLOBRawValue(column)
	case "NUMBER":
		if column.IsDecimalNumber() {
			return new(sql.NullString), nil
		}
		return new(sql.NullInt64), nil
	case "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
		if column.OracleBaseType() == "FLOAT" && column.FloatBitSize() == 0 {
			return new(sql.NullString), nil
		}
		return new(sql.NullFloat64), nil
	case "RAW", "LONG RAW", "BLOB":
		return new([]byte), nil
	case "INTERVAL YEAR TO MONTH":
		return new(string), nil
	case "INTERVAL DAY TO SECOND":
		return new(string), nil
	case "DATE", "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP WITH TIME ZONE":
		return new(sql.NullTime), nil
	default:
		return nil, xerrors.Errorf("Unsupported type '%v'", column.OracleType())
	}
}

func createCLOBRawValue(column *oracle_schema.Column) (interface{}, error) {
	clobAsBLOB, err := column.OracleTable().OracleSchema().OracleDatabase().CLOBAsBLOB()
	if err != nil {
		return "", xerrors.Errorf("CLOB as BLOB strategy error: %w", err)
	}
	if clobAsBLOB {
		return new([]byte), nil
	}
	return new(string), nil
}

func createRawValues(table *oracle_schema.Table) ([]interface{}, error) {
	rawValues := make([]interface{}, table.ColumnsCount())
	for i := 0; i < table.ColumnsCount(); i++ {
		column := table.OracleColumn(i)
		rawValue, err := createRawValue(column)
		if err != nil {
			return nil, xerrors.Errorf("Can't create raw value for column '%v': %w", column.OracleSQLName(), err)
		}
		rawValues[i] = rawValue
	}
	return rawValues, nil
}

//nolint:descriptiveerrors
func castRawValue(rawValue interface{}, column *oracle_schema.Column) (interface{}, error) {
	switch column.OracleBaseType() {
	case
		"ROWID", "UROWID",
		"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "LONG",
		"JSON":
		return castStringValue(rawValue)
	case "CLOB", "NCLOB":
		return castCLOBValue(rawValue, column)
	case "NUMBER":
		if column.IsDecimalNumber() {
			return castDecimalStringValue(rawValue)
		}
		return castIntValue(rawValue, column)
	case "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
		if column.OracleBaseType() == "FLOAT" && column.FloatBitSize() == 0 {
			return castBigFloatStringValue(rawValue)
		}
		return castFloatValue(rawValue, column)
	case "RAW", "LONG RAW", "BLOB":
		return castBytesValue(rawValue)
	case "INTERVAL YEAR TO MONTH":
		return castStringValue(rawValue)
	case "INTERVAL DAY TO SECOND":
		return castDSIntervalStringValue(rawValue)
	case "DATE":
		return castDateValue(rawValue)
	case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE":
		return castTimestampValue(rawValue)
	case "TIMESTAMP WITH TIME ZONE":
		return castTimestampTZValue(rawValue)
	default:
		return nil, xerrors.Errorf("Unsupported type '%v'", column.OracleType())
	}
}

//nolint:descriptiveerrors
func castCLOBValue(rawValue interface{}, column *oracle_schema.Column) (interface{}, error) {
	clobAsBLOB, err := column.OracleTable().OracleSchema().OracleDatabase().CLOBAsBLOB()
	if err != nil {
		return nil, xerrors.Errorf("CLOB as BLOB strategy error: %w", err)
	}
	if clobAsBLOB {
		return castBytesAsStringValue(rawValue)
	}
	return castStringValue(rawValue)
}

func castBytesAsStringValue(rawValue interface{}) (interface{}, error) {
	rawBytes, ok := rawValue.(*[]byte)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*[]byte' type, but got '%T'", rawValue)
	}
	if rawBytes == nil || *rawBytes == nil {
		return nil, nil
	}
	valueBytes := make([]byte, len(*rawBytes))
	copy(valueBytes, *rawBytes)
	return string(valueBytes), nil
}

func castBytesValue(rawValue interface{}) (interface{}, error) {
	rawBytes, ok := rawValue.(*[]byte)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*[]byte' type, but got '%T'", rawValue)
	}
	if rawBytes == nil || *rawBytes == nil {
		return nil, nil
	}
	valueBytes := make([]byte, len(*rawBytes))
	copy(valueBytes, *rawBytes)
	return valueBytes, nil
}

func castStringValue(rawValue interface{}) (interface{}, error) {
	rawString, ok := rawValue.(*string)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*string' type, but got '%T'", rawValue)
	}
	if oracle_common.IsNullString(rawString) {
		return nil, nil
	}
	return *rawString, nil
}

func castIntValue(rawValue interface{}, column *oracle_schema.Column) (interface{}, error) {
	rawNullInt64, ok := rawValue.(*sql.NullInt64)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullInt64' type, but got '%T'", rawValue)
	}
	if !rawNullInt64.Valid {
		return nil, nil
	}
	dt := column.DataType()
	switch dt {
	case oracle_schema.YTTypeInt8:
		return int8(rawNullInt64.Int64), nil
	case oracle_schema.YTTypeInt16:
		return int16(rawNullInt64.Int64), nil
	case oracle_schema.YTTypeInt32:
		return int32(rawNullInt64.Int64), nil
	case oracle_schema.YTTypeInt64:
		return rawNullInt64.Int64, nil
	default:
		return nil, xerrors.Errorf("Unsupported int data type '%v' for column '%v'", dt, column.FullName())
	}
}

func castFloatValue(rawValue interface{}, column *oracle_schema.Column) (interface{}, error) {
	rawNullFloat64, ok := rawValue.(*sql.NullFloat64)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullFloat64' type, but got '%T'", rawValue)
	}
	if !rawNullFloat64.Valid {
		return nil, nil
	}
	switch column.OracleBaseType() {
	case "BINARY_FLOAT", "FLOAT":
		if column.OracleBaseType() == "BINARY_FLOAT" || column.FloatBitSize() == 32 {
			return float32(rawNullFloat64.Float64), nil
		}
		return rawNullFloat64.Float64, nil
	case "BINARY_DOUBLE":
		return rawNullFloat64.Float64, nil
	default:
		return rawNullFloat64.Float64, nil
	}
}

// castDSIntervalStringValue parses the VARCHAR2 representation of INTERVAL DAY TO SECOND
// produced by a server-side CAST, e.g. "+02 10:20:30.123456", into time.Duration.
// Oracle zero-pads days to column precision and fractional seconds to second precision.
func castDSIntervalStringValue(rawValue interface{}) (interface{}, error) {
	rawString, ok := rawValue.(*string)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*string' type, but got '%T'", rawValue)
	}
	if oracle_common.IsNullString(rawString) {
		return nil, nil
	}
	s := *rawString
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimLeft(s, "+-")

	// Format after stripping sign: "DD HH:MI:SS.FFFFFF"
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == ':' || r == '.'
	})
	if len(parts) != 5 {
		return nil, xerrors.Errorf("Can't parse INTERVAL DAY TO SECOND from '%v'", *rawString)
	}

	days, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse days in '%v': %w", *rawString, err)
	}
	hours, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse hours in '%v': %w", *rawString, err)
	}
	minutes, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse minutes in '%v': %w", *rawString, err)
	}
	seconds, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse seconds in '%v': %w", *rawString, err)
	}
	// Normalize fractional seconds to 9 digits (nanoseconds).
	frac := parts[4]
	for len(frac) < 9 {
		frac += "0"
	}
	nanoseconds, err := strconv.ParseInt(frac[:9], 10, 64)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse fractional seconds in '%v': %w", *rawString, err)
	}

	total := days*24*int64(time.Hour) +
		hours*int64(time.Hour) +
		minutes*int64(time.Minute) +
		seconds*int64(time.Second) +
		nanoseconds
	if negative {
		total = -total
	}
	return time.Duration(total), nil
}

func castTimeValueCore(rawValue interface{}) (*time.Time, error) {
	rawNullTime, ok := rawValue.(*sql.NullTime)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullTime' type, but got '%T'", rawValue)
	}
	if !rawNullTime.Valid {
		return nil, nil
	}
	t := rawNullTime.Time
	return &t, nil
}

func castDateValue(rawValue interface{}) (interface{}, error) {
	valueTime, err := castTimeValueCore(rawValue)
	if err != nil {
		return nil, err
	}
	if valueTime == nil {
		return nil, nil
	}
	// Oracle DATE has no timezone — it is just wall-clock digits.
	// godror attaches a Location (from DSN or time.Local) when constructing
	// time.Time, but that Location is a Go artifact. Reinterpret the wall-clock
	// fields in time.UTC so that represent.go's .UTC() call (applied to datetime
	// in the INSERT path) does not shift the value.
	t := time.Date(valueTime.Year(), valueTime.Month(), valueTime.Day(),
		valueTime.Hour(), valueTime.Minute(), valueTime.Second(), valueTime.Nanosecond(), time.UTC)
	return t, nil
}

func castTimestampValue(rawValue interface{}) (interface{}, error) {
	valueTime, err := castTimeValueCore(rawValue)
	if err != nil {
		return nil, err
	}
	if valueTime == nil {
		return nil, nil
	}
	return *valueTime, nil
}

func castTimestampTZValue(rawValue interface{}) (interface{}, error) {
	valueTime, err := castTimeValueCore(rawValue)
	if err != nil {
		return nil, err
	}
	if valueTime == nil {
		return nil, nil
	}
	return *valueTime, nil
}

func castDecimalStringValue(rawValue interface{}) (interface{}, error) {
	rawNullString, ok := rawValue.(*sql.NullString)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullString' type, but got: '%T'", rawValue)
	}
	if !rawNullString.Valid {
		return nil, nil
	}
	return rawNullString.String, nil
}

func castBigFloatStringValue(rawValue interface{}) (interface{}, error) {
	return castDecimalStringValue(rawValue)
}

// buildChangeItemValues casts a scanned row to values for abstract.ChangeItem.ColumnValues.
func buildChangeItemValues(table *oracle_schema.Table, rawValues []interface{}) ([]interface{}, error) {
	out := make([]interface{}, len(rawValues))
	for i := 0; i < len(rawValues); i++ {
		column := table.OracleColumn(i)
		v, err := castRawValue(rawValues[i], column)
		if err != nil {
			return nil, xerrors.Errorf("Can't cast value for column '%v': %w", column.FullName(), err)
		}
		out[i] = v
	}
	return out, nil
}
