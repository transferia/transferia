package schema

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
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
	dataType             string // YT schema type string, e.g. "int64", "utf8"
	oldColumn            *abstract.ColSchema
	convertNumberToInt64 bool
	dataDefault          string
}

func NewColumn(table *Table, row *ColumnRow, convertNumberToInt64 bool) (*Column, error) {
	column := &Column{
		table:                table,
		name:                 row.ColumnName,
		oracleType:           "",
		oracleBaseType:       "",
		dataLength:           row.DataLength,
		dataPrecision:        nil,
		dataScale:            nil,
		nullable:             row.Nullable == nil || *row.Nullable != "N",
		virtual:              false,
		dataType:             "",
		oldColumn:            nil,
		convertNumberToInt64: convertNumberToInt64,
		dataDefault:          "",
	}

	if row.DataType == nil {
		return nil, xerrors.Errorf("Null data type for column '%v'", column.OracleSQLName())
	}
	column.oracleType = *row.DataType

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
	}
	column.virtual = *row.Virtual == "YES"

	column.fillOracleBaseType()
	if err := column.fillDataType(); err != nil {
		return nil, xerrors.Errorf("Type error for column '%v': %w", column.OracleSQLName(), err)
	}

	return column, nil
}

func (column *Column) fillOracleBaseType() {
	builder := strings.Builder{}
	parenthesesCounter := 0
	for _, char := range column.oracleType {
		if char == '(' {
			parenthesesCounter++
		} else if char == ')' {
			parenthesesCounter--
			builder.WriteRune(' ')
		} else if parenthesesCounter == 0 {
			builder.WriteRune(char)
		}
	}
	words := strings.Fields(builder.String())
	column.oracleBaseType = strings.Join(words, " ")
}

func (column *Column) fillDataType() error {
	switch column.oracleBaseType {
	case
		"ROWID", "UROWID",
		"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "LONG",
		"CLOB", "NCLOB",
		"JSON":
		column.dataType = YTTypeString
	case "NUMBER":
		if column.dataPrecision == nil {
			if column.convertNumberToInt64 {
				column.dataType = YTTypeInt64
			} else {
				column.dataType = YTTypeString
			}
		} else {
			if column.dataScale != nil && *column.dataScale > 0 {
				// fractional NUMBER always maps to string regardless of the flag
				column.dataType = YTTypeString
			} else if !column.convertNumberToInt64 {
				// integer NUMBER with explicit precision: respect the flag
				column.dataType = YTTypeString
			} else {
				digitsCount := *column.dataPrecision - column.DataScale()
				switch {
				case digitsCount < 3:
					column.dataType = YTTypeInt8
				case digitsCount < 5:
					column.dataType = YTTypeInt16
				case digitsCount < 10:
					column.dataType = YTTypeInt32
				case digitsCount < 19:
					column.dataType = YTTypeInt64
				default:
					column.dataType = YTTypeString
				}
			}
		}
	case "FLOAT":
		if column.dataPrecision == nil {
			column.dataType = YTTypeFloat64
		} else {
			switch {
			case *column.dataPrecision <= 32:
				column.dataType = YTTypeFloat32
			case *column.dataPrecision <= 64:
				column.dataType = YTTypeFloat64
			default:
				column.dataType = YTTypeString
			}
		}
	case "BINARY_FLOAT":
		column.dataType = YTTypeFloat32
	case "BINARY_DOUBLE":
		column.dataType = YTTypeFloat64
	case "RAW", "LONG RAW", "BLOB":
		column.dataType = YTTypeBytes
	case "DATE":
		column.dataType = YTTypeDatetime
	case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE":
		column.dataType = YTTypeTimestamp
	case "TIMESTAMP WITH TIME ZONE":
		column.dataType = YTTypeTimestamp
	case "INTERVAL YEAR TO MONTH":
		// INTERVAL YEAR TO MONTH has no fixed-length representation in nanoseconds
		// (months differ in length), so it is transferred as the Oracle string form "+YY-MM".
		column.dataType = YTTypeString
	case "INTERVAL DAY TO SECOND":
		column.dataType = YTTypeInterval
	default:
		return xerrors.Errorf("Unsupported type '%v'", column.oracleType)
	}
	return nil
}

// DataType returns the YT schema type string used in abstract.ColSchema.DataType.
func (column *Column) DataType() string {
	return column.dataType
}

// IsDecimalNumber reports whether NUMBER is transferred as decimal text (sql.NullString), not as integer.
func (column *Column) IsDecimalNumber() bool {
	if column.oracleBaseType != "NUMBER" {
		return false
	}
	if column.dataPrecision == nil {
		return !column.convertNumberToInt64
	}
	if column.dataScale != nil && *column.dataScale > 0 {
		return true
	}
	if !column.convertNumberToInt64 {
		return true
	}
	digitsCount := *column.dataPrecision - column.DataScale()
	return digitsCount >= 19
}

// FloatBitSize returns 32, 64, or 0 (big float → string) for FLOAT columns.
func (column *Column) FloatBitSize() int {
	if column.oracleBaseType != "FLOAT" || column.dataPrecision == nil {
		return 64
	}
	switch {
	case *column.dataPrecision <= 32:
		return 32
	case *column.dataPrecision <= 64:
		return 64
	default:
		return 0
	}
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
		}
		return column.OracleSQLName(), nil
	case "TIMESTAMP WITH TIME ZONE":
		// SYS_EXTRACT_UTC converts TSTZ to plain TIMESTAMP in UTC, computed server-side.
		// This avoids ORA-01805: Oracle Instant Client TZD file lookup fails for region-name
		// timezones (e.g. 'UTC', 'America/New_York') when TZD files are absent or version-mismatched.
		// The moment in time is preserved; only the timezone label is normalised to UTC.
		return fmt.Sprintf("SYS_EXTRACT_UTC(%[1]v) as %[1]v", column.OracleSQLName()), nil
	case "TIMESTAMP WITH LOCAL TIME ZONE":
		// TSLTZ is stored as UTC; CAST to TSTZ attaches the current session TZ label, then
		// SYS_EXTRACT_UTC strips it back to UTC — same treatment as TSTZ and equally avoids
		// client-side TZ conversion (ORA-25137 on session/system TZ discrepancy).
		return fmt.Sprintf("SYS_EXTRACT_UTC(CAST(%[1]v AS TIMESTAMP WITH TIME ZONE)) as %[1]v", column.OracleSQLName()), nil
	case "NCLOB":
		// godror's OCI path for SQLT_NCLOB diverges from SQLT_CLOB; convert server-side to CLOB
		// so the column comes back as a standard LOB that godror reads via OCILobRead2.
		return fmt.Sprintf("TO_CLOB(%[1]v) as %[1]v", column.OracleSQLName()), nil
	case "BLOB":
		// godror reads BLOB locators via OCILobRead2 and materialises them as []byte on Scan,
		// the same path already used for CLOB. The previous DBMS_LOB.SUBSTR(col, 32767, 1) wrap
		// returned SQL RAW, which is capped at 2000 bytes on MAX_STRING_SIZE=STANDARD databases
		// (ORA-06502 "raw variable length too long") and at 32767 on EXTENDED — both insufficient
		// for arbitrary BLOBs. Per-batch memory is bounded by the byte-aware flush in
		// snapshot/loader.go.
		return column.OracleSQLName(), nil
	case "ROWID":
		// godror reads ROWID via dpiRowid_getStringValue, which segfaults under AS OF SCN flashback
		// queries (the rowid handle is sometimes returned as 0x0 by ODPI-C even when isNull is false).
		// ROWIDTOCHAR converts the rowid to its 18-char base64 form server-side, so godror sees a
		// regular VARCHAR2 column and avoids the rowid C path entirely.
		return fmt.Sprintf("ROWIDTOCHAR(%[1]v) as %[1]v", column.OracleSQLName()), nil
	case "UROWID":
		// Same rationale as ROWID — but ROWIDTOCHAR does not accept UROWID, so cast to VARCHAR2.
		// Max UROWID length is 4000 bytes (logical rowids of index-organized tables can be long).
		return fmt.Sprintf("CAST(%[1]v AS VARCHAR2(4000)) as %[1]v", column.OracleSQLName()), nil
	case "INTERVAL YEAR TO MONTH":
		// godror does not expose IntervalYM as int64/time.Duration; cast server-side to VARCHAR2
		// to get the canonical Oracle string form, e.g. "+000005-03".
		return fmt.Sprintf("CAST(%[1]v AS VARCHAR2(20)) as %[1]v", column.OracleSQLName()), nil
	case "INTERVAL DAY TO SECOND":
		// OCI rejects scanning INTERVAL DAY TO SECOND into INT8 (ORA-25137); cast server-side
		// to VARCHAR2 to get "+DD HH:MI:SS.FFFFFF" which is then parsed into time.Duration.
		return fmt.Sprintf("CAST(%[1]v AS VARCHAR2(50)) as %[1]v", column.OracleSQLName()), nil
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

func (column *Column) SetDataDefault(val string) {
	column.dataDefault = val
}

func (column *Column) Name() string {
	return oracle_common.ConvertOracleName(column.OracleName())
}

func (column *Column) FullName() string {
	return oracle_common.CreateSQLName(column.OracleTable().OracleSchema().Name(), column.OracleTable().Name(), column.Name())
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
		//nolint:exhaustivestruct
		column.oldColumn = &abstract.ColSchema{
			TableSchema:  column.OracleTable().Schema(),
			TableName:    column.OracleTable().Name(),
			Path:         "",
			ColumnName:   column.Name(),
			DataType:     column.dataType,
			PrimaryKey:   column.Key(),
			FakeKey:      false,
			Required:     !column.Nullable(),
			Expression:   "",
			OriginalType: "oracle:" + column.OracleType(),
		}
		if trimmed := strings.TrimSpace(column.dataDefault); trimmed != "" && !strings.EqualFold(trimmed, "NULL") {
			column.oldColumn.AddProperty(changeitem.DefaultPropertyKey, trimmed)
		}
	}
	return column.oldColumn, nil
}
