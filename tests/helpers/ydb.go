package helpers

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"go.ytsaurus.tech/yt/go/schema"
)

func YDBInitChangeItem(tablePath string) *abstract.ChangeItem {
	currChangeItem := &abstract.ChangeItem{
		ID:         0,
		LSN:        0,
		CommitTime: 0,
		Kind:       abstract.InsertKind,
		Schema:     "",
		Table:      tablePath,
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{PrimaryKey: true, Required: false, ColumnName: "id", DataType: "uint64", OriginalType: "ydb:Uint64"},

			{PrimaryKey: false, Required: false, ColumnName: "Bool_", DataType: string(schema.TypeBoolean), OriginalType: "ydb:Bool"},

			{PrimaryKey: false, Required: false, ColumnName: "Int8_", DataType: string(schema.TypeInt8), OriginalType: "ydb:Int8"},
			{PrimaryKey: false, Required: false, ColumnName: "Int16_", DataType: string(schema.TypeInt16), OriginalType: "ydb:Int16"},
			{PrimaryKey: false, Required: false, ColumnName: "Int32_", DataType: string(schema.TypeInt32), OriginalType: "ydb:Int32"},
			{PrimaryKey: false, Required: false, ColumnName: "Int64_", DataType: string(schema.TypeInt64), OriginalType: "ydb:Int64"},

			{PrimaryKey: false, Required: false, ColumnName: "Uint8_", DataType: string(schema.TypeUint8), OriginalType: "ydb:Uint8"},
			{PrimaryKey: false, Required: false, ColumnName: "Uint16_", DataType: string(schema.TypeUint16), OriginalType: "ydb:Uint16"},
			{PrimaryKey: false, Required: false, ColumnName: "Uint32_", DataType: string(schema.TypeUint32), OriginalType: "ydb:Uint32"},
			{PrimaryKey: false, Required: false, ColumnName: "Uint64_", DataType: string(schema.TypeUint64), OriginalType: "ydb:Uint64"},

			{PrimaryKey: false, Required: false, ColumnName: "Float_", DataType: string(schema.TypeFloat32), OriginalType: "ydb:Float"},
			{PrimaryKey: false, Required: false, ColumnName: "Double_", DataType: string(schema.TypeFloat64), OriginalType: "ydb:Double"},
			{PrimaryKey: false, Required: false, ColumnName: "Decimal_", DataType: string(schema.TypeString), OriginalType: "ydb:Decimal"}, // When used in table columns, precision is fixed: Decimal(22,9)
			{PrimaryKey: false, Required: false, ColumnName: "DyNumber_", DataType: string(schema.TypeString), OriginalType: "ydb:DyNumber"},

			{PrimaryKey: false, Required: false, ColumnName: "String_", DataType: string(schema.TypeBytes), OriginalType: "ydb:String"},
			{PrimaryKey: false, Required: false, ColumnName: "Utf8_", DataType: string(schema.TypeString), OriginalType: "ydb:Utf8"},
			{PrimaryKey: false, Required: false, ColumnName: "Json_", DataType: string(schema.TypeAny), OriginalType: "ydb:Json"},
			{PrimaryKey: false, Required: false, ColumnName: "JsonDocument_", DataType: string(schema.TypeAny), OriginalType: "ydb:JsonDocument"},
			//{PrimaryKey: false, Required: false, ColumnName: "Yson_", DataType: "", OriginalType: "ydb:Yson"}, // can't find any acceptable value
			//{PrimaryKey: false, Required: false, ColumnName: "Uuid_", DataType: "", OriginalType: "ydb:Uuid"}, // Не поддержан для столбцов таблиц

			{PrimaryKey: false, Required: false, ColumnName: "Date_", DataType: string(schema.TypeDate), OriginalType: "ydb:Date"},
			{PrimaryKey: false, Required: false, ColumnName: "Datetime_", DataType: string(schema.TypeDatetime), OriginalType: "ydb:Datetime"},
			{PrimaryKey: false, Required: false, ColumnName: "Timestamp_", DataType: string(schema.TypeTimestamp), OriginalType: "ydb:Timestamp"},
			{PrimaryKey: false, Required: false, ColumnName: "Interval_", DataType: string(schema.TypeInterval), OriginalType: "ydb:Interval"},
			//{PrimaryKey: false, Required: false, ColumnName: "TzDate_", DataType: "", OriginalType: "ydb:TzDate"}, // Не поддержан для столбцов таблиц
			//{PrimaryKey: false, Required: false, ColumnName: "TzDateTime_", DataType: "", OriginalType: "ydb:TzDateTime"}, // Не поддержан для столбцов таблиц
			//{PrimaryKey: false, Required: false, ColumnName: "TzTimestamp_", DataType: "", OriginalType: "ydb:TzTimestamp"}, // Не поддержан для столбцов таблиц
		}),
		ColumnNames: []string{
			"id",
			"Bool_",
			"Int8_",
			"Int16_",
			"Int32_",
			"Int64_",
			"Uint8_",
			"Uint16_",
			"Uint32_",
			"Uint64_",
			"Float_",
			"Double_",
			"Decimal_",
			"DyNumber_",
			"String_",
			"Utf8_",
			"Json_",
			"JsonDocument_",
			//"Yson_", // can't find any acceptable value
			//"Uuid_", // Не поддержан для столбцов таблиц
			"Date_",
			"Datetime_",
			"Timestamp_",
			"Interval_",
			//"TzDate_", // Не поддержан для столбцов таблиц
			//"TzDateTime_", // Не поддержан для столбцов таблиц
			//"TzTimestamp_", // Не поддержан для столбцов таблиц
		},
		ColumnValues: []interface{}{
			1,                //"id",
			true,             //"Bool_",
			int8(1),          //"Int8_",
			int16(2),         //"Int16_",
			int32(3),         //"Int32_",
			int64(4),         //"Int64_",
			uint8(5),         //"Uint8_",
			uint16(6),        //"Uint16_",
			uint32(7),        //"Uint32_",
			uint64(8),        //"Uint64_",
			float32(1.1),     //"Float_",
			2.2,              //"Double_",
			"234.000000000",  //"Decimal_",
			".123e3",         //"DyNumber_",
			[]byte{1},        //"String_",
			"my_utf8_string", //"Utf8_",
			"{}",             //"Json_",
			"{}",             //"JsonDocument_",
			//"Yson_", // can't find any acceptable value
			//"Uuid_", // Не поддержан для столбцов таблиц
			time.Date(2020, 2, 2, 0, 0, 0, 0, time.UTC),   //"Date_",
			time.Date(2020, 2, 2, 10, 2, 22, 0, time.UTC), //"Datetime_",
			time.Date(2020, 2, 2, 10, 2, 22, 0, time.UTC), //"Timestamp_",
			time.Duration(123000),                         //"Interval_",
			//"TzDate_", // Не поддержан для столбцов таблиц
			//"TzDateTime_", // Не поддержан для столбцов таблиц
			//"TzTimestamp_", // Не поддержан для столбцов таблиц
		},
	}

	for i := range currChangeItem.ColumnNames {
		if currChangeItem.ColumnNames[i] == "Json_" || currChangeItem.ColumnNames[i] == "JsonDocument_" {
			var val interface{}
			_ = json.Unmarshal([]byte(currChangeItem.ColumnValues[i].(string)), &val)
			currChangeItem.ColumnValues[i] = val
		}
	}

	return currChangeItem
}

//---

func YDBStmtInsert(t *testing.T, tablePath string, id int) *abstract.ChangeItem {
	result := YDBInitChangeItem(tablePath)

	require.Greater(t, len(result.ColumnNames), 0)
	require.Equal(t, "id", result.ColumnNames[0])

	result.ColumnValues[0] = id

	require.False(t, result.KeysChanged())
	return result
}

func YDBStmtInsertNulls(t *testing.T, tablePath string, id int) *abstract.ChangeItem {
	result := YDBInitChangeItem(tablePath)

	require.Greater(t, len(result.ColumnNames), 0)
	require.Equal(t, "id", result.ColumnNames[0])

	result.ColumnValues[0] = id
	for i := 1; i < len(result.ColumnValues); i++ {
		result.ColumnValues[i] = nil
	}

	require.False(t, result.KeysChanged())
	return result
}

func YDBStmtUpdate(t *testing.T, tablePath string, id int, newInt32Val int) *abstract.ChangeItem {
	result := YDBInitChangeItem(tablePath)

	require.Greater(t, len(result.ColumnNames), 5)
	require.Equal(t, "id", result.ColumnNames[0])
	require.Equal(t, "Int32_", result.ColumnNames[4])

	result.ColumnValues[0] = id
	result.ColumnValues[4] = newInt32Val

	require.False(t, result.KeysChanged())
	return result
}

func YDBStmtUpdateTOAST(t *testing.T, tablePath string, id int, newInt32Val int) *abstract.ChangeItem {
	result := YDBInitChangeItem(tablePath)

	require.Greater(t, len(result.ColumnNames), 5)
	require.Equal(t, "id", result.ColumnNames[0])
	require.Equal(t, "Int32_", result.ColumnNames[4])

	result.ColumnValues[0] = id
	result.ColumnValues[4] = newInt32Val

	result.ColumnNames = result.ColumnNames[0:5]
	result.ColumnValues = result.ColumnValues[0:5]

	require.False(t, result.KeysChanged())
	return result
}

func YDBStmtDelete(t *testing.T, tablePath string, id int) *abstract.ChangeItem {
	result := YDBInitChangeItem(tablePath)

	require.Greater(t, len(result.ColumnNames), 0)
	require.Equal(t, "id", result.ColumnNames[0])

	result.Kind = abstract.DeleteKind
	result.ColumnValues[0] = id
	result.ColumnNames = result.ColumnNames[0:1]
	result.ColumnValues = result.ColumnValues[0:1]
	result.OldKeys = abstract.OldKeysType{
		KeyNames:  []string{"id"},
		KeyTypes:  []string{"int"},
		KeyValues: []interface{}{id},
	}

	require.False(t, result.KeysChanged())
	return result
}

func YDBStmtDeleteCompoundKey(t *testing.T, tablePath string, ids ...any) *abstract.ChangeItem {
	result := YDBInitChangeItem(tablePath)

	require.Greater(t, len(ids), 0)
	require.Greater(t, len(result.ColumnNames), len(ids))

	result.Kind = abstract.DeleteKind
	result.ColumnValues = ids
	result.ColumnNames = result.ColumnNames[0:len(ids)]
	result.ColumnValues = result.ColumnValues[0:len(ids)]
	result.OldKeys = abstract.OldKeysType{
		KeyNames:  result.ColumnNames,
		KeyTypes:  yslices.Map(result.TableSchema.Columns()[0:len(ids)], func(col abstract.ColSchema) string { return col.DataType }),
		KeyValues: ids,
	}

	require.False(t, result.KeysChanged())
	return result
}

func YDBTwoTablesEqual(t *testing.T, token, database, instance, tableA, tableB string) {
	tableAData := YDBPullDataFromTable(t, token, database, instance, tableA)
	tableBData := YDBPullDataFromTable(t, token, database, instance, tableB)
	require.Equal(t, len(tableAData), len(tableBData))
	sort.Slice(tableAData, func(i, j int) bool {
		return strings.Join(tableAData[i].KeyVals(), ".") < strings.Join(tableAData[j].KeyVals(), ".")
	})
	sort.Slice(tableBData, func(i, j int) bool {
		return strings.Join(tableBData[i].KeyVals(), ".") < strings.Join(tableBData[j].KeyVals(), ".")
	})
	for i := 0; i < len(tableAData); i++ {
		changeItemA, changeItemB := tableAData[i], tableBData[i]
		changeItemA.CommitTime = 0
		changeItemA.Table = "!"
		changeItemA.PartID = ""
		changeItemAStr := changeItemA.ToJSONString()
		changeItemB.CommitTime = 0
		changeItemB.Table = "!"
		changeItemB.PartID = ""
		changeItemBStr := changeItemB.ToJSONString()
		require.Equal(t, changeItemAStr, changeItemBStr)
	}
}

func YDBPullDataFromTable(t *testing.T, token, database, instance, table string) []abstract.ChangeItem {
	src := &ydb.YdbSource{
		Token:              model.SecretString(token),
		Database:           database,
		Instance:           instance,
		Tables:             []string{table},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		SecurityGroupIDs:   nil,
		Underlay:           false,
		ServiceAccountID:   "",
		UseFullPaths:       true,
		SAKeyContent:       "",
		ChangeFeedMode:     "",
		BufferSize:         0,
	}
	sinkMock := &MockSink{}
	targetMock := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       model.DisabledCleanup,
	}
	transferMock := MakeTransfer("fake", src, &targetMock, abstract.TransferTypeSnapshotOnly)

	var extracted []abstract.ChangeItem

	sinkMock.PushCallback = func(input []abstract.ChangeItem) {
		for _, currItem := range input {
			if currItem.Kind == abstract.InsertKind {
				require.NotZero(t, len(currItem.KeyCols()))
				extracted = append(extracted, currItem)
			}
		}
	}
	Activate(t, transferMock)
	return extracted
}

// Test values
func YDBStmtInsertValues(t *testing.T, tablePath string, values []interface{}, id int) *abstract.ChangeItem {
	result := YDBInitChangeItem(tablePath)
	result.ColumnValues = values
	require.Equal(t, len(result.ColumnNames), len(result.ColumnValues))
	require.Greater(t, len(result.ColumnNames), 0)
	require.Equal(t, "id", result.ColumnNames[0])

	result.ColumnValues[0] = id

	require.False(t, result.KeysChanged())
	return result
}
func YDBStmtInsertValuesMultikey(t *testing.T, tablePath string, values []any, ids ...any) *abstract.ChangeItem {
	result := YDBInitChangeItem(tablePath)
	result.ColumnValues = values
	require.Equal(t, len(result.ColumnNames), len(result.ColumnValues))
	require.Greater(t, len(result.ColumnNames), 0)
	require.Greater(t, len(ids), 0)
	require.Greater(t, len(values), len(ids))

	for i, id := range ids {
		result.ColumnValues[i] = id
		result.TableSchema.Columns()[i].PrimaryKey = true
	}

	require.False(t, result.KeysChanged())
	return result
}

var (
	YDBTestValues1 = []interface{}{
		2,
		false,
		int8(1),
		int16(2),
		int32(3),
		int64(4),
		uint8(5),
		uint16(6),
		uint32(8),
		uint64(9),
		float32(21.1),
		22.2,
		"234.000000001",
		"1.123e3",
		[]byte{2},
		"other_utf_8_string",
		map[string]interface{}{"1": 1},
		map[string]interface{}{"2": 2},
		time.Date(2022, 2, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2022, 2, 2, 10, 2, 22, 0, time.UTC),
		time.Date(2022, 2, 2, 10, 2, 22, 0, time.UTC),
		time.Duration(234000),
	}

	YDBTestValues2 = []interface{}{
		3,
		true,
		int8(4),
		int16(5),
		int32(6),
		int64(7),
		uint8(8),
		uint16(9),
		uint32(10),
		uint64(11),
		float32(21.1),
		32.2,
		"1234.000000001",
		".223e3",
		[]byte{4},
		"utf8_string",
		map[string]interface{}{"3": 6},
		map[string]interface{}{"4": 5},
		time.Date(2023, 2, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 2, 2, 10, 2, 22, 0, time.UTC),
		time.Date(2023, 2, 2, 10, 2, 22, 0, time.UTC),
		time.Duration(423000),
	}

	YDBTestValues3 = []interface{}{
		4,
		false,
		int8(9),
		int16(11),
		int32(21),
		int64(31),
		uint8(41),
		uint16(51),
		uint32(71),
		uint64(81),
		float32(1.2),
		2.4,
		"4.000000000",
		"8.323e3",
		[]byte{9},
		"4_string_string",
		map[string]interface{}{"8": 5},
		map[string]interface{}{"7": 2},
		time.Date(2025, 2, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 2, 2, 10, 2, 22, 0, time.UTC),
		time.Date(2025, 2, 2, 10, 2, 22, 0, time.UTC),
		time.Duration(321000),
	}

	YDBTestMultikeyValues1 = []interface{}{
		1,
		false,
		int8(127),
		int16(32767),
		int32(2147483647),
		int64(9223372036854775807),
		uint8(255),
		uint16(65535),
		uint32(4294967295),
		uint64(18446744073709551615),
		float32(9999.9999),
		9999999999.999999,
		"99999999999999999999999.99999999999999999999999999999999999999999999999",
		"1.123e3",
		[]byte{8, 8, 0, 0, 5, 5, 5, 3, 5, 3, 5},
		"Bobr kurwa",
		map[string]interface{}{"a": -1},
		map[string]interface{}{"b": 2},
		time.Date(2024, 4, 8, 18, 38, 0, 0, time.UTC),
		time.Date(2024, 4, 8, 18, 38, 22, 0, time.UTC),
		time.Date(2024, 4, 8, 18, 38, 44, 0, time.UTC),
		time.Duration(4291747200000000 - 1), // this is the largest possible: https://github.com/transferia/transferia/arcadia/contrib/ydb/core/ydb_convert/ydb_convert.cpp?rev=r13809522#L445
	}

	YDBTestMultikeyValues2 = []interface{}{
		2,
		false,
		int8(-128),
		int16(-32768),
		int32(-2147483648),
		int64(-9223372036854775808),
		uint8(0),
		uint16(0),
		uint32(0),
		uint64(0),
		float32(-0.000001),
		-0.000000000000000001,
		"-0.0000000000000000000000000000000000000001",
		"1.123e3",
		[]byte{8, 80, 0, 55, 5, 35, 35},
		"Ja pierdole",
		map[string]interface{}{"x": 1, "y": -2},
		map[string]interface{}{"x": -2, "y": -1},
		time.Date(1970, 1, 1, 1, 1, 1, 1, time.UTC),
		time.Date(1970, 1, 1, 1, 1, 1, 2, time.UTC),
		time.Date(1970, 1, 1, 1, 1, 1, 3, time.UTC),
		time.Duration(-4291747200000000 + 1), // this is the largest possible: https://github.com/transferia/transferia/arcadia/contrib/ydb/core/ydb_convert/ydb_convert.cpp?rev=r13809522#L445
	}

	YDBTestMultikeyValues3 = []interface{}{
		2,
		true,
		int8(8),
		int16(8),
		int32(0),
		int64(0),
		uint8(5),
		uint16(5),
		uint32(5),
		uint64(5),
		float32(3.5),
		3.5,
		"8800.5553535",
		"1.123e3",
		[]byte{8, 8, 00, 5, 55, 35, 35},
		"prosche pozvonit chem u kogo-to zanimat",
		map[string]interface{}{"foo": 146, "bar": -238},
		map[string]interface{}{"fizz": -64, "buzz": 63},
		time.Date(2022, 6, 27, 0, 0, 0, 0, time.UTC),
		time.Date(2022, 6, 28, 0, 2, 40, 0, time.UTC),
		time.Date(2022, 6, 29, 0, 5, 20, 0, time.UTC),
		24*time.Hour + 2*time.Minute + 40*time.Second,
	}
)
