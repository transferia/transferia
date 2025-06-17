//go:build !disable_ydb_provider

package ydb

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
)

func checkChangeItemValidForDebeziumEmitter(t *testing.T, changeItem *abstract.ChangeItem) {
	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName: "public",
		debeziumparameters.TopicPrefix:    "my_topic",
		debeziumparameters.SourceType:     "ydb",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	arrKV, err := emitter.EmitKV(changeItem, time.Time{}, false, nil)
	require.NoError(t, err)

	for _, kv := range arrKV {
		fmt.Println(kv.DebeziumKey)
		if kv.DebeziumVal == nil {
			fmt.Println("NULL")
		} else {
			fmt.Println(*kv.DebeziumVal)
		}
	}
}

func TestConvertToChangeItem(t *testing.T) {
	schema := abstract.NewTableSchema(abstract.TableColumns{
		{ColumnName: "id", DataType: "uint64", PrimaryKey: true, OriginalType: "ydb:Uint64"},
		{ColumnName: "Bool_", DataType: "boolean", PrimaryKey: false, OriginalType: "ydb:Bool"},
		{ColumnName: "Int8_", DataType: "int8", PrimaryKey: false, OriginalType: "ydb:Int8"},
		{ColumnName: "Int16_", DataType: "int16", PrimaryKey: false, OriginalType: "ydb:Int16"},
		{ColumnName: "Int32_", DataType: "int32", PrimaryKey: false, OriginalType: "ydb:Int32"},
		{ColumnName: "Int64_", DataType: "int64", PrimaryKey: false, OriginalType: "ydb:Int64"},
		{ColumnName: "Uint8_", DataType: "uint8", PrimaryKey: false, OriginalType: "ydb:Uint8"},
		{ColumnName: "Uint16_", DataType: "uint16", PrimaryKey: false, OriginalType: "ydb:Uint16"},
		{ColumnName: "Uint32_", DataType: "uint32", PrimaryKey: false, OriginalType: "ydb:Uint32"},
		{ColumnName: "Uint64_", DataType: "uint64", PrimaryKey: false, OriginalType: "ydb:Uint64"},
		{ColumnName: "Float_", DataType: "float", PrimaryKey: false, OriginalType: "ydb:Float"},
		{ColumnName: "Double_", DataType: "double", PrimaryKey: false, OriginalType: "ydb:Double"},
		{ColumnName: "Decimal_", DataType: "utf8", PrimaryKey: false, OriginalType: "ydb:Decimal"},
		{ColumnName: "DyNumber_", DataType: "utf8", PrimaryKey: false, OriginalType: "ydb:DyNumber"},
		{ColumnName: "String_", DataType: "string", PrimaryKey: false, OriginalType: "ydb:String"},
		{ColumnName: "Utf8_", DataType: "utf8", PrimaryKey: false, OriginalType: "ydb:Utf8"},
		{ColumnName: "Json_", DataType: "any", PrimaryKey: false, OriginalType: "ydb:Json"},
		{ColumnName: "JsonDocument_", DataType: "any", PrimaryKey: false, OriginalType: "ydb:JsonDocument"},
		{ColumnName: "Uuid_", DataType: "string", PrimaryKey: false, OriginalType: "ydb:Uuid"},
		{ColumnName: "Date_", DataType: "date", PrimaryKey: false, OriginalType: "ydb:Date"},
		{ColumnName: "Datetime_", DataType: "datetime", PrimaryKey: false, OriginalType: "ydb:Datetime"},
		{ColumnName: "Timestamp_", DataType: "timestamp", PrimaryKey: false, OriginalType: "ydb:Timestamp"},
		{ColumnName: "Interval_", DataType: "interval", PrimaryKey: false, OriginalType: "ydb:Interval"},
	})

	event := cdcEvent{
		Key: []interface{}{
			json.Number("2"),
		},
		Update: map[string]interface{}{
			"Bool_":         true,
			"Date_":         "2020-02-02T00:00:00.000000Z",
			"Datetime_":     "2020-02-02T10:02:22.000000Z",
			"Decimal_":      "234",
			"Double_":       json.Number("2.2"),
			"DyNumber_":     ".123e3",
			"Float_":        json.Number("1.100000024"),
			"Int8_":         json.Number("1"),
			"Int16_":        json.Number("2"),
			"Int32_":        json.Number("3"),
			"Int64_":        json.Number("4"),
			"Interval_":     json.Number("123"),
			"JsonDocument_": map[string]interface{}{},
			"Json_":         map[string]interface{}{},
			"Uuid_":         "6af014ea-29dd-401c-a7e3-68a58305f4fb",
			"String_":       "AQ==",
			"Timestamp_":    "2020-02-02T10:02:22.000000Z",
			"Uint8_":        json.Number("5"),
			"Uint16_":       json.Number("6"),
			"Uint32_":       json.Number("7"),
			"Uint64_":       json.Number("8"),
			"Utf8_":         "my_utf8_string",
		},
		Erase:    nil,
		NewImage: nil,
		OldImage: nil,
	}

	changeItem, err := convertToChangeItem("topic_path", schema, &event, time.Time{}, 0, 0, 1, false)
	require.NoError(t, err)

	fmt.Println(changeItem.ToJSONString())

	checkChangeItemValidForDebeziumEmitter(t, changeItem)

	t.Run("FillMissingFieldsWithNulls", func(t *testing.T) {
		event.Update = map[string]interface{}{}

		changeItem, err := convertToChangeItem("topic_path", schema, &event, time.Time{}, 0, 0, 1, true)
		require.NoError(t, err)

		fmt.Println(changeItem.ToJSONString())

		checkChangeItemValidForDebeziumEmitter(t, changeItem)
	})
}

func TestAddZerosToDecimal(t *testing.T) {
	testcases := map[string]string{
		"0":           "0.000000000",
		"0.000000001": "0.000000001",
		"123.123":     "123.123000000",
		"321.001230":  "321.001230000",
	}
	for inData, outData := range testcases {
		require.Equal(t, outData, string(addZerosToDecimal([]byte(inData))))
	}
}

func TestDifferentJSON(t *testing.T) {
	schema := abstract.NewTableSchema(abstract.TableColumns{
		{ColumnName: "id", DataType: "uint64", PrimaryKey: true, OriginalType: "ydb:Uint64"},
		{ColumnName: "Json_", DataType: "any", PrimaryKey: false, OriginalType: "ydb:Json"},
		{ColumnName: "JsonDocument_", DataType: "any", PrimaryKey: false, OriginalType: "ydb:JsonDocument"},
	})

	testCounter := 1
	for testName, testValue := range map[string]interface{}{
		"map":    map[string]interface{}{"eat": "bulki"},
		"array":  []interface{}{1, "hello"},
		"number": "88005553535.0",
		"string": "\"Hello, world!\"",
		"bool":   "true",
		"null":   "null",
	} {
		fmt.Printf("test '%v'\n", testName)
		event := cdcEvent{
			Key: []interface{}{
				json.Number(fmt.Sprintf("%d", testCounter)),
			},
			Update: map[string]interface{}{
				"JsonDocument_": testValue,
				"Json_":         testValue,
			},
			Erase:    nil,
			NewImage: nil,
			OldImage: nil,
		}

		changeItem, err := convertToChangeItem("topic_path", schema, &event, time.Time{}, 0, 0, 1, false)
		require.NoError(t, err)

		fmt.Println(changeItem.ToJSONString())

		checkChangeItemValidForDebeziumEmitter(t, changeItem)
	}
}

func TestComplexPkey(t *testing.T) {
	event := cdcEvent{
		Key:      []interface{}{"MTQ3NDU3MTc1MQ==", "Mjg5Y2FhNDM2NzVjMTFlZWE4ZWZiZWIzMzJkZmYyODI="},
		Update:   nil,
		Erase:    map[string]interface{}{},
		NewImage: nil,
		OldImage: nil,
	}
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			ColumnName:   "user_id",
			DataType:     "string",
			PrimaryKey:   true,
			FakeKey:      false,
			Required:     true,
			Expression:   "",
			OriginalType: "ydb:String",
		},
		{
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			ColumnName:   "post_id",
			DataType:     "string",
			PrimaryKey:   true,
			FakeKey:      false,
			Required:     true,
			Expression:   "",
			OriginalType: "ydb:String",
		},
		{
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			ColumnName:   "created_at",
			DataType:     "timestamp",
			PrimaryKey:   false,
			FakeKey:      false,
			Required:     false,
			Expression:   "",
			OriginalType: "ydb:Timestamp",
		},
	})
	item, err := convertToChangeItem("tableName", tableSchema, &event, time.Time{}, 0, 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, 2, len(item.OldKeys.KeyNames))
	require.Equal(t, 2, len(item.OldKeys.KeyValues))
}
