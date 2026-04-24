package schema

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/abstract2/types"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

const (
	defaultNumberPrecision    = 38
	defaultNumberScale        = 127
	defaultFloatPrecision     = 126
	defaultTimestampPrecision = 6
)

type Column struct {
	table                *Table
	name                 string
	oracleType           string
	oracleBaseType       string
	dataLength           int
	dataPrecision        *int
	dataScale            *int
	nullable             bool
	virtual              bool
	transferType         abstract2.Type
	oldColumn            *abstract.ColSchema
	convertNumberToInt64 bool
}

func NewColumn(table *Table, row *ColumnRow, convertNumberToInt64 bool) (*Column, error) {
	column := &Column{
		table:          table,
		name:           row.ColumnName,
		oracleType:     "",
		oracleBaseType: "",
		dataLength:     row.DataLength,
		dataPrecision:  nil,
		dataScale:      nil,
		// The value is N if there is a NOT NULL constraint on the column or if the column is part of a PRIMARY KEY
		nullable:             row.Nullable == nil || *row.Nullable != "N",
		virtual:              false,
		transferType:         nil,
		oldColumn:            nil,
		convertNumberToInt64: convertNumberToInt64,
	}

	if row.DataType == nil {
		return nil, xerrors.Errorf("Null data type for column '%v'", column.OracleSQLName())
	} else {
		column.oracleType = *row.DataType
	}

	if row.DataPrecision != nil {
		dataPrecision := *row.DataPrecision
		column.dataPrecision = &dataPrecision
	}

	if row.DataScale != nil {
		dataScale := *row.DataScale
		column.dataScale = &dataScale
	}

	if row.Virtual == nil {
		return nil, xerrors.Errorf("Null virtual for column '%v'", column.OracleSQLName())
	} else {
		// Indicates whether the column is a virtual column (YES) or not (NO)
		column.virtual = *row.Virtual == "YES"
	}

	column.fillOracleBaseType()
	if err := column.fillTransferType(); err != nil {
		return nil, xerrors.Errorf("Type error for column '%v': %w", column.OracleSQLName(), err)
	}

	return column, nil
}

func (column *Column) fillOracleBaseType() {
	// Remove sizes
	builder := strings.Builder{}
	parenthesesCounter := 0
	for _, char := range column.oracleType {
		if char == '(' {
			parenthesesCounter++
		} else if char == ')' {
			parenthesesCounter--
			// Adding space after close bracket, if this is not here
			builder.WriteRune(' ')
		} else if parenthesesCounter == 0 {
			builder.WriteRune(char)
		}
	}
	// Trim spaces
	words := strings.Fields(builder.String())
	column.oracleBaseType = strings.Join(words, " ")
}

func (column *Column) fillTransferType() error {
	// https://docs.oracle.com/cd/E11882_01/server.112/e41085/sqlqr06002.htm#SQLQR959
	switch column.oracleBaseType {
	case
		"ROWID", "UROWID",
		"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "LONG",
		"CLOB", "NCLOB":
		column.transferType = types.NewStringType(column.dataLength)
	case "NUMBER":
		// DataPrecision is size of number, in decimal digits, can be zero - default case
		// DataScale is offset from "point", can be positive (left shift) and negative (right shift)
		if column.dataPrecision == nil {
			if column.convertNumberToInt64 {
				column.transferType = types.NewInt64Type()
			} else {
				column.transferType = types.NewDecimalType(column.DataPrecision(), column.DataScale())
			}
		} else {
			if *column.dataScale > 0 {
				column.transferType = types.NewDecimalType(column.DataPrecision(), column.DataScale())
			} else {
				digitsCount := *column.dataPrecision - *column.dataScale
				switch {
				case digitsCount < 3:
					column.transferType = types.NewInt8Type()
				case digitsCount < 5:
					column.transferType = types.NewInt16Type()
				case digitsCount < 10:
					column.transferType = types.NewInt32Type()
				case digitsCount < 19:
					column.transferType = types.NewInt64Type()
				default:
					column.transferType = types.NewDecimalType(column.DataPrecision(), column.DataScale())
				}
			}
		}
	case "FLOAT":
		// DataPrecision is size of number, in binary digits (bits), can be zero - default case
		if column.dataPrecision == nil {
			column.transferType = types.NewDoubleType()
		} else {
			switch {
			case *column.dataPrecision <= 32:
				column.transferType = types.NewFloatType()
			case *column.dataPrecision <= 64:
				column.transferType = types.NewDoubleType()
			default:
				column.transferType = types.NewBigFloatType(column.DataPrecision())
			}
		}
	case "BINARY_FLOAT":
		column.transferType = types.NewFloatType()
	case "BINARY_DOUBLE":
		column.transferType = types.NewDoubleType()
	case "RAW", "LONG RAW", "BLOB":
		column.transferType = types.NewBytesType()
	case "DATE":
		column.transferType = types.NewDateTimeType()
	case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE":
		column.transferType = types.NewTimestampType(column.DataPrecision())
	case "TIMESTAMP WITH TIME ZONE":
		column.transferType = types.NewTimestampTZType(column.DataPrecision())
	case "INTERVAL YEAR TO MONTH":
		column.transferType = types.NewIntervalType()
	case "INTERVAL DAY TO SECOND":
		column.transferType = types.NewIntervalType()
	default:
		return xerrors.Errorf("Unsupported type '%v'", column.oracleType)
	}
	return nil
}

func (column *Column) OracleTable() *Table {
	return column.table
}

func (column *Column) OracleName() string {
	return column.name
}

func (column *Column) OracleSQLSelect() (string, error) {
	switch column.oracleBaseType {
	case "CLOB":
		clobAsBLOB, err := column.OracleTable().OracleSchema().OracleDatabase().CLOBAsBLOB()
		if err != nil {
			return "", xerrors.Errorf("CLOB as BLOB strategy error: %w", err)
		}
		if clobAsBLOB {
			return fmt.Sprintf("%[1]v(%[2]v) as %[2]v", oracle_common.CLOBToBLOBFunctionName, column.OracleSQLName()), nil
		} else {
			return column.OracleSQLName(), nil
		}
	default:
		return column.OracleSQLName(), nil
	}
}

func (column *Column) OracleSQLName() string {
	return oracle_common.CreateSQLName(column.OracleName())
}

func (column *Column) OracleType() string {
	return column.oracleType
}

func (column *Column) OracleBaseType() string {
	return column.oracleBaseType
}

func (column *Column) DataLength() int {
	return column.dataLength
}

func (column *Column) DefaultDataPrecision() bool {
	return column.dataPrecision == nil
}

func (column *Column) DataPrecision() int {
	if column.dataPrecision != nil {
		return *column.dataPrecision
	}

	switch column.oracleBaseType {
	case "NUMBER":
		return defaultNumberPrecision
	case "FLOAT":
		return defaultFloatPrecision
	case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP WITH TIME ZONE":
		return defaultTimestampPrecision
	default:
		return 0
	}
}

func (column *Column) DefaultDataScale() bool {
	return column.dataScale == nil
}

func (column *Column) DataScale() int {
	if column.dataScale != nil {
		return *column.dataScale
	}

	switch column.oracleBaseType {
	case "NUMBER":
		return defaultNumberScale
	default:
		return 0
	}
}

func (column *Column) IsVirtual() bool {
	return column.virtual
}

// Begin of base Table interface

func (column *Column) Table() abstract2.Table {
	return column.OracleTable()
}

func (column *Column) Name() string {
	return oracle_common.ConvertOracleName(column.OracleName())
}

func (column *Column) FullName() string {
	return oracle_common.CreateSQLName(column.OracleTable().OracleSchema().Name(), column.OracleTable().Name(), column.Name())
}

func (column *Column) Type() abstract2.Type {
	return column.transferType
}

func (column *Column) Value(val interface{}) (abstract2.Value, error) {
	panic("implement me")
}

func (column *Column) Nullable() bool {
	return column.nullable
}

func (column *Column) Key() bool {
	keyIndex := column.OracleTable().OracleKeyIndex()
	if keyIndex == nil {
		return false
	}
	return keyIndex.HasColumn(column)
}

func (column *Column) ToOldColumn() (*abstract.ColSchema, error) {
	if column.oldColumn == nil {
		oldType, err := column.transferType.ToOldType()
		if err != nil {
			return nil, xerrors.Errorf("Column '%v' cannot be converted to old format: %w", column.OracleSQLName(), err)
		}
		//nolint:exhaustivestruct
		column.oldColumn = &abstract.ColSchema{
			TableSchema:  column.Table().Schema(),
			TableName:    column.Table().Name(),
			Path:         "",
			ColumnName:   column.Name(),
			DataType:     string(oldType),
			PrimaryKey:   column.Key(),
			FakeKey:      false,
			Required:     !column.Nullable(),
			Expression:   "",
			OriginalType: "oracle:" + column.OracleType(),
		}
	}
	return column.oldColumn, nil
}

// End of base Table interface
