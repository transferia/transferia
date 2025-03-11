package adapter

import (
	"fmt"
	"strings"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/base/types"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type legacyTableAdapter struct {
	cols        *abstract.TableSchema
	table       abstract.TableID
	colNamesIDX map[string]int
}

func (l *legacyTableAdapter) Database() string {
	return "no database"
}

func (l *legacyTableAdapter) Schema() string {
	return l.table.Namespace
}

func (l *legacyTableAdapter) Name() string {
	return l.table.Name
}

func (l *legacyTableAdapter) FullName() string {
	return l.table.Fqtn()
}

func (l *legacyTableAdapter) ColumnsCount() int {
	return len(l.cols.Columns())
}

func (l *legacyTableAdapter) Column(i int) base.Column {
	return &legacyColumnAdapter{
		table: l,
		col:   l.cols.Columns()[i],
	}
}

func (l *legacyTableAdapter) ColumnByName(name string) base.Column {
	return &legacyColumnAdapter{
		col:   l.cols.Columns()[l.colNamesIDX[name]],
		table: l,
	}
}

func (l *legacyTableAdapter) ToOldTable() (*abstract.TableSchema, error) {
	return l.cols, nil
}

type legacyColumnAdapter struct {
	col   abstract.ColSchema
	table base.Table
}

func (l legacyColumnAdapter) Value(val interface{}) (base.Value, error) {
	if val == nil {
		switch l.Type().(type) {
		case *types.CompositeType:
			return types.NewDefaultCompositeValue(nil, l), nil
		case *types.BigFloatType:
			return types.NewDefaultBigFloatValue(nil, l), nil
		case *types.BoolType:
			return types.NewDefaultBoolValue(nil, l), nil
		case *types.BytesType:
			return types.NewDefaultBytesValue(nil, l), nil
		case *types.DateType:
			return types.NewDefaultDateValue(nil, l), nil
		case *types.DateTimeType:
			return types.NewDefaultDateTimeValue(nil, l), nil
		case *types.DecimalType:
			return types.NewDefaultDecimalValue(nil, l), nil
		case *types.DoubleType:
			return types.NewDefaultDoubleValue(nil, l), nil
		case *types.FloatType:
			return types.NewDefaultFloatValue(nil, l), nil
		case *types.Int8Type:
			return types.NewDefaultInt8Value(nil, l), nil
		case *types.Int16Type:
			return types.NewDefaultInt16Value(nil, l), nil
		case *types.Int32Type:
			return types.NewDefaultInt32Value(nil, l), nil
		case *types.Int64Type:
			return types.NewDefaultInt64Value(nil, l), nil
		case *types.IntervalType:
			return types.NewDefaultIntervalValue(nil, l), nil
		case *types.StringType:
			return types.NewDefaultStringValue(nil, l), nil
		case *types.TimestampTZType:
			return types.NewDefaultTimestampTZValue(nil, l), nil
		case *types.TimestampType:
			return types.NewDefaultTimestampValue(nil, l), nil
		case *types.UInt8Type:
			return types.NewDefaultUInt8Value(nil, l), nil
		case *types.UInt16Type:
			return types.NewDefaultUInt16Value(nil, l), nil
		case *types.UInt32Type:
			return types.NewDefaultUInt32Value(nil, l), nil
		case *types.UInt64Type:
			return types.NewDefaultUInt64Value(nil, l), nil
		default:
			return nil, xerrors.Errorf("unknown type: %T for column: %v", l.Type(), l.FullName())
		}
	}
	switch l.Type().(type) {
	case *types.CompositeType:
		return types.NewDefaultCompositeValue(val, l), nil
	case *types.BigFloatType:
		switch v := val.(type) {
		case string:
			return types.NewDefaultBigFloatValue(&v, l), nil
		}
	case *types.BoolType:
		switch v := val.(type) {
		case bool:
			return types.NewDefaultBoolValue(&v, l), nil
		}
	case *types.BytesType:
		switch v := val.(type) {
		case []byte:
			return types.NewDefaultBytesValue(v, l), nil
		}
	case *types.DateType:
		switch v := val.(type) {
		case *time.Time:
			return types.NewDefaultDateValue(v, l), nil
		case time.Time:
			return types.NewDefaultDateValue(&v, l), nil
		}
	case *types.DateTimeType:
		switch v := val.(type) {
		case *time.Time:
			return types.NewDefaultDateTimeValue(v, l), nil
		case time.Time:
			return types.NewDefaultDateTimeValue(&v, l), nil
		case uint64:
			// must be seconds
			unixTime := time.Unix(int64(v), 0)
			return types.NewDefaultDateTimeValue(&unixTime, l), nil
		}
	case *types.DecimalType:
		switch v := val.(type) {
		case string:
			return types.NewDefaultDecimalValue(&v, l), nil
		}
	case *types.DoubleType:
		switch v := val.(type) {
		case float64:
			return types.NewDefaultDoubleValue(&v, l), nil
		}
	case *types.FloatType:
		switch v := val.(type) {
		case float32:
			return types.NewDefaultFloatValue(&v, l), nil
		}
	case *types.Int8Type:
		switch v := val.(type) {
		case int8:
			return types.NewDefaultInt8Value(&v, l), nil
		}
	case *types.Int16Type:
		switch v := val.(type) {
		case int16:
			return types.NewDefaultInt16Value(&v, l), nil
		}
	case *types.Int32Type:
		switch v := val.(type) {
		case int:
			vv := int32(v)
			return types.NewDefaultInt32Value(&vv, l), nil
		case int32:
			return types.NewDefaultInt32Value(&v, l), nil
		}
	case *types.Int64Type:
		switch v := val.(type) {
		case int:
			vv := int64(v)
			return types.NewDefaultInt64Value(&vv, l), nil
		case int64:
			return types.NewDefaultInt64Value(&v, l), nil
		}
	case *types.IntervalType:
		switch v := val.(type) {
		case *time.Duration:
			return types.NewDefaultIntervalValue(v, l), nil
		case time.Duration:
			return types.NewDefaultIntervalValue(&v, l), nil
		case int64:
			// value must be in microseconds
			duration := time.Duration(int64(v) * 1000)
			return types.NewDefaultIntervalValue(&duration, l), nil
		}
	case *types.StringType:
		switch v := val.(type) {
		case string:
			return types.NewDefaultStringValue(&v, l), nil
		}
	case *types.TimestampTZType:
		switch v := val.(type) {
		case *time.Time:
			return types.NewDefaultTimestampTZValue(v, l), nil
		case time.Time:
			return types.NewDefaultTimestampTZValue(&v, l), nil
		}
	case *types.TimestampType:
		switch v := val.(type) {
		case *time.Time:
			return types.NewDefaultTimestampValue(v, l), nil
		case time.Time:
			return types.NewDefaultTimestampValue(&v, l), nil
		case uint64:
			// Must be microseconds
			unixTime := time.Unix(int64(v)/int64(1000000), (int64(v)%int64(1000000))*int64(1000))
			return types.NewDefaultTimestampValue(&unixTime, l), nil
		}
	case *types.UInt8Type:
		switch v := val.(type) {
		case uint8:
			return types.NewDefaultUInt8Value(&v, l), nil
		}
	case *types.UInt16Type:
		switch v := val.(type) {
		case uint16:
			return types.NewDefaultUInt16Value(&v, l), nil
		}
	case *types.UInt32Type:
		switch v := val.(type) {
		case int:
			vv := uint32(v)
			return types.NewDefaultUInt32Value(&vv, l), nil
		case uint32:
			return types.NewDefaultUInt32Value(&v, l), nil
		}
	case *types.UInt64Type:
		switch v := val.(type) {
		case uint64:
			return types.NewDefaultUInt64Value(&v, l), nil
		}
	default:
		return nil, xerrors.Errorf("unknown type: %T for column: %v", l.Type(), l.FullName())
	}
	return nil, xerrors.Errorf("unknown value type: %T for column: %v (%T)", val, l.FullName(), l.Type())
}

func (l legacyColumnAdapter) Table() base.Table {
	return l.table
}

func (l legacyColumnAdapter) Name() string {
	return l.col.ColumnName
}

func (l legacyColumnAdapter) FullName() string {
	return l.col.ColumnName
}

func (l legacyColumnAdapter) Type() base.Type {
	switch yt_schema.Type(strings.ToLower(l.col.DataType)) {
	case yt_schema.TypeInt8:
		return types.NewInt8Type()
	case yt_schema.TypeInt16:
		return types.NewInt16Type()
	case yt_schema.TypeInt32:
		return types.NewInt32Type()
	case yt_schema.TypeInt64:
		return types.NewInt64Type()

	case yt_schema.TypeUint8:
		return types.NewUInt8Type()
	case yt_schema.TypeUint16:
		return types.NewUInt16Type()
	case yt_schema.TypeUint32:
		return types.NewUInt32Type()
	case yt_schema.TypeUint64:
		return types.NewUInt64Type()

	case yt_schema.TypeFloat32:
		return types.NewFloatType()
	case yt_schema.TypeFloat64:
		return types.NewDoubleType()

	case yt_schema.TypeBytes:
		return types.NewStringType(-1)

	case yt_schema.TypeString:
		return types.NewStringType(-1)

	case yt_schema.TypeBoolean:
		return types.NewBoolType()

	case yt_schema.TypeAny:
		return types.NewCompositeType()

	case yt_schema.TypeDate:
		return types.NewDateTimeType()
	case yt_schema.TypeDatetime:
		return types.NewDateTimeType()

	case yt_schema.TypeTimestamp:
		return types.NewTimestampType(6)

	case yt_schema.TypeInterval:
		return types.NewIntervalType()
	}
	panic(fmt.Sprintf("should never happened, data type: %v", l.col.DataType))
}

func (l legacyColumnAdapter) Nullable() bool {
	return !l.col.Required
}

func (l legacyColumnAdapter) Key() bool {
	return l.col.PrimaryKey
}

func (l legacyColumnAdapter) ToOldColumn() (*abstract.ColSchema, error) {
	return &l.col, nil
}

func NewTableFromLegacy(cols *abstract.TableSchema, table abstract.TableID) base.Table {
	colNamesIDX := map[string]int{}
	for i, col := range cols.Columns() {
		colNamesIDX[col.ColumnName] = i
	}
	return &legacyTableAdapter{
		colNamesIDX: colNamesIDX,
		cols:        cols,
		table:       table,
	}
}
