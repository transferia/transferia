package snapshot

import (
	"database/sql"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/abstract2/events"
	"github.com/transferia/transferia/pkg/abstract2/types"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
)

//nolint:descriptiveerrors
func createRawValue(column *oracle_schema.Column) (interface{}, error) {
	switch column.OracleBaseType() {
	case
		"ROWID", "UROWID",
		"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "LONG",
		"NCLOB":
		return new(string), nil
	case "CLOB":
		return createCLOBRawValue(column)
	case "NUMBER":
		switch column.Type().(type) {
		case *types.DecimalType:
			return new(sql.NullString), nil
		default:
			return new(sql.NullInt64), nil
		}
	case "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
		switch column.Type().(type) {
		case *types.BigFloatType:
			return new(sql.NullString), nil
		default:
			return new(sql.NullFloat64), nil
		}
	case "RAW", "LONG RAW", "BLOB":
		return new([]byte), nil
	case "INTERVAL DAY TO SECOND":
		return new(sql.NullInt64), nil
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
	} else {
		return new(string), nil
	}
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
func castRawValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	switch column.OracleBaseType() {
	case
		"ROWID", "UROWID",
		"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "LONG",
		"NCLOB":
		return castStringValue(rawValue, column)
	case "CLOB":
		return castCLOBValue(rawValue, column)
	case "NUMBER":
		switch column.Type().(type) {
		case *types.DecimalType:
			return castDecimalValue(rawValue, column)
		default:
			return castIntValue(rawValue, column)
		}
	case "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
		switch column.Type().(type) {
		case *types.BigFloatType:
			return castBigFloatValue(rawValue, column)
		default:
			return castFloatValue(rawValue, column)
		}
	case "RAW", "LONG RAW", "BLOB":
		return castBytesValue(rawValue, column)
	case "INTERVAL DAY TO SECOND":
		return castIntervalValue(rawValue, column)
	case "DATE":
		return castDateValue(rawValue, column)
	case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE":
		return castTimestampValue(rawValue, column)
	case "TIMESTAMP WITH TIME ZONE":
		return castTimestampTZValue(rawValue, column)
	default:
		return nil, xerrors.Errorf("Unsupported type '%v'", column.OracleType())
	}
}

func castCLOBValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	clobAsBLOB, err := column.OracleTable().OracleSchema().OracleDatabase().CLOBAsBLOB()
	if err != nil {
		return nil, xerrors.Errorf("CLOB as BLOB strategy error: %w", err)
	}
	if clobAsBLOB {
		//nolint:descriptiveerrors
		return castBytesAsStringValue(rawValue, column)
	} else {
		//nolint:descriptiveerrors
		return castStringValue(rawValue, column)
	}
}

func castBytesAsStringValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	rawBytes, ok := rawValue.(*[]byte)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*[]byte' type, but got '%T'", rawValue)
	}

	if rawBytes == nil || *rawBytes == nil {
		return types.NewDefaultStringValue(nil, column), nil
	}

	valueBytes := make([]byte, len(*rawBytes))
	copy(valueBytes, *rawBytes)
	valueString := string(valueBytes)
	return types.NewDefaultStringValue(&valueString, column), nil
}

func castBytesValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	rawBytes, ok := rawValue.(*[]byte)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*[]byte' type, but got '%T'", rawValue)
	}

	if rawBytes == nil || *rawBytes == nil {
		return types.NewDefaultBytesValue(nil, column), nil
	}

	valueBytes := make([]byte, len(*rawBytes))
	copy(valueBytes, *rawBytes)
	return types.NewDefaultBytesValue(valueBytes, column), nil
}

func castStringValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	rawString, ok := rawValue.(*string)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*string' type, but got '%T'", rawValue)
	}

	if oracle_common.IsNullString(rawString) {
		return types.NewDefaultStringValue(nil, column), nil
	}

	valueString := *rawString
	return types.NewDefaultStringValue(&valueString, column), nil
}

func castIntValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	rawNullInt64, ok := rawValue.(*sql.NullInt64)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullInt64' type, but got '%T'", rawValue)
	}

	switch column.Type().(type) {
	case *types.Int8Type:
		if !rawNullInt64.Valid {
			return types.NewDefaultInt8Value(nil, column), nil
		}
		valueInt8 := int8(rawNullInt64.Int64)
		return types.NewDefaultInt8Value(&valueInt8, column), nil
	case *types.Int16Type:
		if !rawNullInt64.Valid {
			return types.NewDefaultInt16Value(nil, column), nil
		}
		valueInt16 := int16(rawNullInt64.Int64)
		return types.NewDefaultInt16Value(&valueInt16, column), nil
	case *types.Int32Type:
		if !rawNullInt64.Valid {
			return types.NewDefaultInt32Value(nil, column), nil
		}
		valueInt32 := int32(rawNullInt64.Int64)
		return types.NewDefaultInt32Value(&valueInt32, column), nil
	case *types.Int64Type:
		if !rawNullInt64.Valid {
			return types.NewDefaultInt64Value(nil, column), nil
		}
		valueInt64 := rawNullInt64.Int64
		return types.NewDefaultInt64Value(&valueInt64, column), nil
	default:
		return nil, xerrors.Errorf("Unsupported sub type '%v' for type '%v'. columnName: %v", column.OracleType(), column.Type(), column.FullName())
	}
}

func castFloatValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	rawNullFloat64, ok := rawValue.(*sql.NullFloat64)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullFloat64' type, but got '%T'", rawValue)
	}

	switch column.Type().(type) {
	case *types.FloatType:
		if !rawNullFloat64.Valid {
			return types.NewDefaultFloatValue(nil, column), nil
		}
		valueFloat := float32(rawNullFloat64.Float64)
		return types.NewDefaultFloatValue(&valueFloat, column), nil
	case *types.DoubleType:
		if !rawNullFloat64.Valid {
			return types.NewDefaultDoubleValue(nil, column), nil
		}
		valueDouble := rawNullFloat64.Float64
		return types.NewDefaultDoubleValue(&valueDouble, column), nil
	default:
		return nil, xerrors.Errorf("Unsupported sub type '%v' for type '%v'. columnName: %v", column.OracleType(), column.Type(), column.FullName())
	}
}

func castIntervalValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	rawNullInt64, ok := rawValue.(*sql.NullInt64)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullInt64' type, but got: '%T'", rawValue)
	}

	if !rawNullInt64.Valid {
		return types.NewDefaultIntervalValue(nil, column), nil
	}

	valueDuration := time.Duration(rawNullInt64.Int64)
	return types.NewDefaultIntervalValue(&valueDuration, column), nil
}

func castTimeValueCore(rawValue interface{}) (*time.Time, error) {
	rawNullTime, ok := rawValue.(*sql.NullTime)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullTime' type, but got '%T'", rawValue)
	}

	if !rawNullTime.Valid {
		return nil, nil
	}

	valueTime := rawNullTime.Time
	return &valueTime, nil
}

func castDateValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	valueTime, err := castTimeValueCore(rawValue)
	if err != nil {
		return nil, err
	}

	// In Oracle 'date' is 'datetime' value
	return types.NewDefaultDateTimeValue(valueTime, column), nil
}

func castTimestampValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	valueTime, err := castTimeValueCore(rawValue)
	if err != nil {
		return nil, err
	}

	return types.NewDefaultTimestampValue(valueTime, column), nil
}

func castTimestampTZValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	valueTime, err := castTimeValueCore(rawValue)
	if err != nil {
		return nil, err
	}

	return types.NewDefaultTimestampTZValue(valueTime, column), nil
}

func castDecimalValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	rawNullString, ok := rawValue.(*sql.NullString)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullString' type, but got: '%T'", rawValue)
	}

	if !rawNullString.Valid {
		return types.NewDefaultDecimalValue(nil, column), nil
	}

	valueString := rawNullString.String
	return types.NewDefaultDecimalValue(&valueString, column), nil
}

func castBigFloatValue(rawValue interface{}, column *oracle_schema.Column) (abstract2.Value, error) {
	rawNullString, ok := rawValue.(*sql.NullString)
	if !ok {
		return nil, xerrors.Errorf("Expected value of '*sql.NullString' type, but got: '%T'", rawValue)
	}

	if !rawNullString.Valid {
		return types.NewDefaultBigFloatValue(nil, column), nil
	}

	valueString := rawNullString.String
	return types.NewDefaultBigFloatValue(&valueString, column), nil
}

func createInsertEvent(table *oracle_schema.Table, rawValues []interface{}, position abstract2.LogPosition) (events.InsertEvent, error) {
	event := events.NewDefaultLoggedInsertEvent(table, position)
	for i := 0; i < len(rawValues); i++ {
		column := table.OracleColumn(i)
		value, err := castRawValue(rawValues[i], column)
		if err != nil {
			return nil, xerrors.Errorf("Can't cast value for column '%v': %w", column.FullName(), err)
		}
		if err := event.AddNewValue(value); err != nil {
			return nil, err
		}
	}
	return event, nil
}
