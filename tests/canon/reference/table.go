package reference

import (
	"testing"
	"time"

	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/tests/canon/validator"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
)

type Struct struct {
	Int64 int64  `yson:"s_int64"`
	Utf8  string `yson:"s_utf8"`
}

type Row struct {
	Key       int64  `yson:"t_int64_key"`
	SystemKey int64  `yson:"t_int64_system_key"`
	Int8      int8   `yson:"t_int8"`
	Int16     int16  `yson:"t_int16"`
	Int32     int32  `yson:"t_int32"`
	Int64     int64  `yson:"t_int64"`
	UInt8     uint8  `yson:"t_uint8"`
	UInt16    uint16 `yson:"t_uint16"`
	UInt32    uint32 `yson:"t_uint32"`
	UInt64    uint64 `yson:"t_uint64"`

	Float  float32 `yson:"t_float"`
	Double float64 `yson:"t_double"`
	Bool   bool    `yson:"t_bool"`

	String string `yson:"t_string"`
	Utf8   string `yson:"t_utf8"`

	Date      time.Time `yson:"t_date"`
	DateTime  time.Time `yson:"t_datetime"`
	Timestamp time.Time `yson:"t_timestamp"`
	//Interval  int64     `yson:"t_interval"`
	Yson string `yson:"t_yson"`

	Decimal   string            `yson:"t_decimal"`
	List      []uint64          `yson:"t_list"`
	Struct    Struct            `yson:"t_struct"`
	Tuple     []interface{}     `yson:"t_tuple"`
	VarTuple  []interface{}     `yson:"t_var_tuple"`
	VarStruct []interface{}     `yson:"t_var_struct"`
	Dict      map[string]string `yson:"t_dict"`
	Tagged    string            `yson:"t_tagged"`
}

func Table() []abstract.ChangeItem {
	cols := abstract.NewTableSchema([]abstract.ColSchema{
		// Primitives
		{ColumnName: "t_int64_key", DataType: string(schema.TypeInt64), PrimaryKey: true},
		{ColumnName: "t_int64_system_key", DataType: string(schema.TypeInt64)},
		{ColumnName: "t_int8", DataType: string(schema.TypeInt8)},
		{ColumnName: "t_int16", DataType: string(schema.TypeInt16)},
		{ColumnName: "t_int32", DataType: string(schema.TypeInt32)},
		{ColumnName: "t_int64", DataType: string(schema.TypeInt64)},
		{ColumnName: "t_uint8", DataType: string(schema.TypeUint8)},
		{ColumnName: "t_uint16", DataType: string(schema.TypeUint16)},
		{ColumnName: "t_uint32", DataType: string(schema.TypeUint32)},
		{ColumnName: "t_uint64", DataType: string(schema.TypeUint64)},
		{ColumnName: "t_float", DataType: string(schema.TypeFloat32)},
		{ColumnName: "t_double", DataType: string(schema.TypeFloat64)},
		{ColumnName: "t_bool", DataType: string(schema.TypeBoolean)},
		{ColumnName: "t_string", DataType: string(schema.TypeBytes)},
		{ColumnName: "t_utf8", DataType: string(schema.TypeString)},
		{ColumnName: "t_date", DataType: string(schema.TypeDate)},
		{ColumnName: "t_datetime", DataType: string(schema.TypeDatetime)},
		{ColumnName: "t_timestamp", DataType: string(schema.TypeTimestamp)},
		//{ColumnName: "t_interval", DataType: schema.TypeInterval},
		{ColumnName: "t_list", DataType: string(schema.TypeAny)},
		{ColumnName: "t_struct", DataType: string(schema.TypeAny)},
		{ColumnName: "t_tuple", DataType: string(schema.TypeAny)},
		{ColumnName: "t_var_tuple", DataType: string(schema.TypeAny)},
		{ColumnName: "t_var_struct", DataType: string(schema.TypeAny)},
		{ColumnName: "t_dict", DataType: string(schema.TypeAny)},
		{ColumnName: "t_tagged", DataType: string(schema.TypeAny)},
		// Complex types
		//{ColumnName: "t_yson", DataType: schema.Optional{Item: schema.TypeAny}},
		//{ ColumnName: "t_decimal"},
	})
	colNames := yslices.Map(cols.Columns(), func(colSchema abstract.ColSchema) string {
		return colSchema.ColumnName
	})
	inRow := Row{
		Key:       123,
		SystemKey: 321,
		Int8:      -10,
		Int16:     -1000,
		Int32:     -1000000,
		Int64:     -10000000000,
		UInt8:     10,
		UInt16:    1000,
		UInt32:    1000000,
		UInt64:    10000000000,
		Float:     1.2,
		Double:    1.2,
		Bool:      true,
		String:    "test string",
		Utf8:      "test utf8",
		Date:      time.Unix(1639652326, 0),
		DateTime:  time.Unix(1639652326, 0),
		Timestamp: time.Unix(0, 1658866078090000000),
		//Interval:  10000000,
		List:      []uint64{100500},
		Struct:    Struct{Int64: 100600, Utf8: "test struct"},
		Tuple:     []interface{}{uint8(10), "test tuple"},
		VarTuple:  []interface{}{0, "test variant (unnamed)"},
		VarStruct: []interface{}{"vs_string", "test variant (named)"},
		Dict:      map[string]string{"key": "value"},
		Tagged:    "test tagged",
		//Yson:      "{\"test\"=\"yson\"}",
		//Decimal:   "\x80\x00\x7A\xB7",
	}

	return []abstract.ChangeItem{{
		ID:          1,
		LSN:         123,
		CommitTime:  0,
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "reference_schema",
		Table:       "reference_tables",
		ColumnNames: colNames,
		TableSchema: cols,
		ColumnValues: []interface{}{
			inRow.Key,
			inRow.SystemKey,
			inRow.Int8,
			inRow.Int16,
			inRow.Int32,
			inRow.Int64,
			inRow.UInt8,
			inRow.UInt16,
			inRow.UInt32,
			inRow.UInt64,
			inRow.Float,
			inRow.Double,
			inRow.Bool,
			inRow.String,
			inRow.Utf8,
			inRow.Date,
			inRow.DateTime,
			inRow.Timestamp,
			//inRow.Interval,
			inRow.List,
			inRow.Struct,
			inRow.Tuple,
			inRow.VarTuple,
			inRow.VarStruct,
			inRow.Dict,
			inRow.Tagged,
		},
	}}
}

func Canon(t *testing.T, source model.Source) {
	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		source,
		&model.MockDestination{
			SinkerFactory: validator.New(model.IsStrictSource(source), validator.ValuesTypeChecker, validator.Referencer(t)),
			Cleanup:       model.DisabledCleanup,
		},
		abstract.TransferTypeSnapshotOnly,
	)
	helpers.Activate(t, transfer)
}
