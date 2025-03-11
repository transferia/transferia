package tests

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestUnparametrizedDecimal(t *testing.T) {
	changeItemStr := `
	{
		"id": 0,
		"nextlsn": 0,
		"commitTime": 0,
		"txPosition": 0,
		"kind": "insert",
		"schema": "",
		"table": "customers3",
		"columnnames": ["i", "n"],
		"columnvalues": [2, 123.456],
		"table_schema": [{
			"path": "",
			"name": "i",
			"type": "int32",
			"key": true,
			"required": false,
			"original_type": "pg:integer"
		}, {
			"path": "",
			"name": "n",
			"type": "double",
			"key": false,
			"required": false,
			"original_type": "pg:numeric"
		}],
		"oldkeys": {},
		"tx_id": "",
		"query": ""
	}`
	changeItem, err := abstract.UnmarshalChangeItem([]byte(changeItemStr))
	require.NoError(t, err)

	// check values

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{
		debeziumparameters.DatabaseDBName: "public",
		debeziumparameters.TopicPrefix:    "my_topic",
		debeziumparameters.SourceType:     "pg",
	})
	afterVals, err := debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)

	require.Equal(t, fmt.Sprintf("%T", map[string]interface{}{}), fmt.Sprintf("%T", afterVals["n"]))
	decimalVal, _ := json.Marshal(afterVals["n"])
	require.NoError(t, err)
	require.Equal(t, `{"scale":3,"value":"AeJA"}`, string(decimalVal))

	// check 123456

	changeItem.ColumnValues[1] = json.Number("123456")
	afterVals, err = debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%T", map[string]interface{}{}), fmt.Sprintf("%T", afterVals["n"]))
	decimalVal, _ = json.Marshal(afterVals["n"])
	require.NoError(t, err)
	require.Equal(t, `{"scale":0,"value":"AeJA"}`, string(decimalVal))
}

func TestZeroDecimal(t *testing.T) {
	changeItem := &abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "val"},
		ColumnValues: []interface{}{1, json.Number("0")},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", OriginalType: "pg:integer"},
			{ColumnName: "val", OriginalType: "pg:numeric(3,2)"},
		}),
	}

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{
		debeziumparameters.DatabaseDBName: "public",
		debeziumparameters.TopicPrefix:    "my_topic",
		debeziumparameters.SourceType:     "pg",
	})
	emitter, err := debezium.NewMessagesEmitter(params, "", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)

	// chain

	currDebeziumKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.NotZero(t, strings.Contains(*currDebeziumKV[0].DebeziumVal, `"val":"AA=="`))
	recoveredChangeItem, err := receiver.Receive(*currDebeziumKV[0].DebeziumVal)
	require.NoError(t, err)

	require.Equal(t, "0", recoveredChangeItem.ColumnValues[1])

	// chain again

	currDebeziumKV, err = emitter.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	recoveredChangeItem, err = receiver.Receive(*currDebeziumKV[0].DebeziumVal)
	require.NoError(t, err)

	require.Equal(t, "0", recoveredChangeItem.ColumnValues[1])
}

func TestPgInterval(t *testing.T) {
	knownOriginalTypes := []string{
		"pg:interval year",
		"pg:interval month",
		"pg:interval day",
		"pg:interval hour",
		"pg:interval minute",
		"pg:interval second",
		"pg:interval year to month",
		"pg:interval day to hour",
		"pg:interval day to minute",
		"pg:interval day to second",
		"pg:interval hour to minute",
		"pg:interval hour to second",
		"pg:interval minute to second",
		"pg:interval(0)",
		"pg:interval(1)",
		"pg:interval(2)",
		"pg:interval(3)",
		"pg:interval(4)",
		"pg:interval(5)",
		"pg:interval(6)",
		"pg:interval second(0)",
		"pg:interval day to second(0)",
		"pg:interval hour to second(0)",
		"pg:interval minute to second(0)",
		"pg:interval second(1)",
		"pg:interval day to second(1)",
		"pg:interval hour to second(1)",
		"pg:interval minute to second(1)",
		"pg:interval second(6)",
		"pg:interval day to second(6)",
		"pg:interval hour to second(6)",
		"pg:interval minute to second(6)",
	}

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{
		debeziumparameters.DatabaseDBName:   "database",
		debeziumparameters.TopicPrefix:      "databaseServerName",
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.SourceType:       "pg",
	})
	for _, currOriginalType := range knownOriginalTypes {
		changeItem := abstract.ChangeItem{
			ColumnNames:  []string{"v"},
			ColumnValues: []interface{}{"00:00:00.000000"},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "v", DataType: ytschema.TypeAny.String(), OriginalType: currOriginalType},
			}),
		}
		myMap, err := debezium.BuildKVMap(&changeItem, params, true)
		require.NoError(t, err)
		require.Len(t, myMap, 1)
		require.Equal(t, uint64(0), myMap["v"])
	}
}

func TestEnum(t *testing.T) {
	var changeItem = &abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "val"},
		ColumnValues: []interface{}{1, "bar"},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), OriginalType: "pg:integer"},
			{ColumnName: "val", DataType: ytschema.TypeString.String(), OriginalType: "pg:my_enum_type", Properties: map[abstract.PropertyKey]interface{}{pgcommon.EnumAllValues: []string{"foo", "bar"}}},
		}),
	}

	//---

	emitterWithOriginalTypes, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	currDebeziumKV, err := emitterWithOriginalTypes.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))

	receiver := debezium.NewReceiver(nil, nil)
	recoveredChangeItem, err := receiver.Receive(*currDebeziumKV[0].DebeziumVal)
	require.NoError(t, err)
	require.Equal(t, "bar", recoveredChangeItem.ColumnValues[1])
	require.Equal(t, "val", recoveredChangeItem.TableSchema.Columns()[1].ColumnName)
	require.Equal(t, "pg:my_enum_type", recoveredChangeItem.TableSchema.Columns()[1].OriginalType)
	require.Equal(t, []string{"foo", "bar"}, recoveredChangeItem.TableSchema.Columns()[1].Properties[pgcommon.EnumAllValues])

	canon.SaveJSON(t, *currDebeziumKV[0].DebeziumVal)
}

func TestNegativeTimestamp(t *testing.T) {
	result, err := time.Parse("2006-01-02T15:04:05-07:00", "1900-01-01T03:00:00+02:30")
	require.NoError(t, err)

	var changeItem = &abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "val"},
		ColumnValues: []interface{}{1, result},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), OriginalType: "pg:integer"},
			{ColumnName: "val", DataType: ytschema.TypeTimestamp.String(), OriginalType: "pg:timestamp without time zone", Properties: map[abstract.PropertyKey]interface{}{pgcommon.DatabaseTimeZone: "W-SU"}},
		}),
	}

	//---

	emitterWithOriginalTypes, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	currDebeziumKV, err := emitterWithOriginalTypes.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))

	msg := currDebeziumKV[0]
	fmt.Printf(msg.DebeziumKey)
	fmt.Printf(*msg.DebeziumVal)

	receiver := debezium.NewReceiver(nil, nil)
	recoveredChangeItem, err := receiver.Receive(*currDebeziumKV[0].DebeziumVal)
	require.NoError(t, err)
	require.Equal(t, "1900-01-01 03:00:00 +0230 MMT", recoveredChangeItem.ColumnValues[1].(time.Time).String())
	require.Equal(t, "val", recoveredChangeItem.TableSchema.Columns()[1].ColumnName)
	require.Equal(t, "pg:timestamp without time zone", recoveredChangeItem.TableSchema.Columns()[1].OriginalType)
	// require.Equal(t, "W-SU", recoveredChangeItem.TableSchema.Columns()[1].Properties[pgcommon.DatabaseTimeZone])

	canon.SaveJSON(t, *currDebeziumKV[0].DebeziumVal)
}
