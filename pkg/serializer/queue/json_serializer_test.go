package queue

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/exp/maps"
)

func TestJSONSerializerEmptyInput(t *testing.T) {
	batcher := model.Batching{
		Enabled:        false,
		Interval:       0,
		MaxChangeItems: 0,
		MaxMessageSize: 0,
	}
	jsonSerializer, err := NewJSONSerializer(batcher, false, logger.Log)
	require.NoError(t, err)

	batches, err := jsonSerializer.Serialize([]abstract.ChangeItem{})
	require.NoError(t, err)
	require.Len(t, batches, 0)
}

func TestJSONSerializerTopicNameAllTypes(t *testing.T) {
	colSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{PrimaryKey: false, ColumnName: "val_int64", DataType: schema.TypeInt64.String()},
		{PrimaryKey: false, ColumnName: "val_int32", DataType: schema.TypeInt32.String()},
		{PrimaryKey: false, ColumnName: "val_int16", DataType: schema.TypeInt16.String()},
		{PrimaryKey: false, ColumnName: "val_int8", DataType: schema.TypeInt8.String()},
		{PrimaryKey: false, ColumnName: "val_uint64", DataType: schema.TypeUint64.String()},
		{PrimaryKey: false, ColumnName: "val_uint32", DataType: schema.TypeUint32.String()},
		{PrimaryKey: false, ColumnName: "val_uint16", DataType: schema.TypeUint16.String()},
		{PrimaryKey: false, ColumnName: "val_uint8", DataType: schema.TypeUint8.String()},
		{PrimaryKey: false, ColumnName: "val_float", DataType: schema.TypeFloat32.String()},
		{PrimaryKey: false, ColumnName: "val_double", DataType: schema.TypeFloat64.String()},

		{PrimaryKey: false, ColumnName: "val_string", DataType: schema.TypeBytes.String()},
		{PrimaryKey: false, ColumnName: "val_utf8", DataType: schema.TypeString.String()},
		{PrimaryKey: false, ColumnName: "val_boolean", DataType: schema.TypeBoolean.String()},

		{PrimaryKey: false, ColumnName: "val_any", DataType: schema.TypeAny.String()},

		{PrimaryKey: false, ColumnName: "val_date", DataType: schema.TypeDate.String()},
		{PrimaryKey: false, ColumnName: "val_datetime", DataType: schema.TypeDatetime.String()},
		{PrimaryKey: false, ColumnName: "val_timestamp", DataType: schema.TypeTimestamp.String()},
		{PrimaryKey: false, ColumnName: "val_interval", DataType: schema.TypeInterval.String()},
	})
	data := map[string]interface{}{
		"val_int64":     int64(-1234567899123456789),
		"val_int32":     int32(-123456789),
		"val_int16":     int16(-12345),
		"val_int8":      int8(-123),
		"val_uint64":    uint64(123456789123456789),
		"val_uint32":    uint32(123456789),
		"val_uint16":    uint16(12345),
		"val_uint8":     uint8(123),
		"val_float":     float32(1.23),
		"val_double":    1.234,
		"val_string":    "bla bla bla",
		"val_utf8":      "utf8 bla bla bla",
		"val_boolean":   true,
		"val_any":       map[string]interface{}{"123": 123, "key": "val"},
		"val_date":      time.Date(2021, 2, 3, 0, 0, 0, 0, time.UTC),
		"val_datetime":  time.Date(2021, 3, 4, 5, 6, 7, 8, time.UTC),
		"val_timestamp": time.Second + 123,
		"val_interval":  time.Second + 321,
	}
	changeItem := abstract.ChangeItemFromMap(
		data,
		colSchema,
		"basic_types15",
		"insert")
	changeItem.Schema = "public"
	batcher := model.Batching{
		Enabled:        false,
		Interval:       0,
		MaxChangeItems: 0,
		MaxMessageSize: 0,
	}

	changeItem0 := changeItem
	changeItem1 := changeItem

	changeItem0.Table = "table0"
	changeItem1.Table = "table1"

	//-------------------------
	// saveTxOrder: false

	jsonSerializer, err := NewJSONSerializer(batcher, false, logger.Log)
	require.NoError(t, err)
	batches, err := jsonSerializer.Serialize([]abstract.ChangeItem{changeItem0, changeItem1})
	require.NoError(t, err)
	require.Len(t, batches, 2)

	type BatchInfo struct {
		Name   string
		Keys   []string
		Values []string
	}
	var canonData []BatchInfo

	batchesKeys := maps.Keys(batches)
	sort.Slice(batchesKeys, func(i, j int) bool {
		return batchesKeys[i].Name < batchesKeys[j].Name
	})

	for _, id := range batchesKeys {
		messages := batches[id]
		var batchInfo BatchInfo
		batchInfo.Name = id.String()
		for _, message := range messages {
			batchInfo.Keys = append(batchInfo.Keys, string(message.Key))
			batchInfo.Values = append(batchInfo.Values, string(message.Value))
		}
		canonData = append(canonData, batchInfo)
	}

	canon.SaveJSON(t, canonData)
	//------------------------
	// saveTxOrder: true

	jsonSerializerTx, err := NewJSONSerializer(batcher, true, logger.Log)
	require.NoError(t, err)
	batchesTx, err := jsonSerializerTx.Serialize([]abstract.ChangeItem{changeItem0, changeItem1})
	require.NoError(t, err)
	require.Len(t, batchesTx, 1)
	for k, v := range batchesTx {
		require.Equal(t, abstract.TablePartID{TableID: abstract.TableID{Namespace: "", Name: ""}, PartID: ""}, k)
		require.Len(t, v, 2)
	}

}
