package debezium

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestDelete(t *testing.T) {
	debeziumMsg := `{"payload":{"after":null,"before":{"a_id":1,"a_name":null},"op":"d","source":{"connector":"postgresql","db":"postgres","lsn":23385168,"name":"__data_transfer_stub","schema":"public","snapshot":"false","table":"__test","ts_ms":1672943646565,"txId":557,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1672943646565},"schema":{"fields":[{"field":"before","fields":[{"field":"a_id","optional":false,"type":"int32"},{"field":"a_name","optional":true,"type":"string"}],"name":"__data_transfer_stub.public.__test.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"a_id","optional":false,"type":"int32"},{"field":"a_name","optional":true,"type":"string"}],"name":"__data_transfer_stub.public.__test.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"__data_transfer_stub.public.__test.Envelope","optional":false,"type":"struct"}}`
	receiver := NewReceiver(nil, nil)
	changeItem, err := receiver.Receive(debeziumMsg)
	require.NoError(t, err)
	require.Equal(t, "int32", changeItem.OldKeys.KeyTypes[0])
	changeItem.OldKeys.KeyTypes[0] = "integer"
	changeItem.CommitTime = 1672944458105963000
	require.Equal(t, `{"id":557,"nextlsn":23385168,"commitTime":1672944458105963000,"txPosition":0,"kind":"delete","schema":"public","table":"__test","part":"","columnnames":null,"table_schema":[{"table_schema":"public","table_name":"__test","path":"","name":"a_id","type":"int32","key":true,"fake_key":false,"required":false,"expression":"","original_type":""},{"table_schema":"public","table_name":"__test","path":"","name":"a_name","type":"utf8","key":false,"fake_key":false,"required":false,"expression":"","original_type":""}],"oldkeys":{"keynames":["a_id"],"keytypes":["integer"],"keyvalues":[1]},"tx_id":"","query":""}`, changeItem.ToJSONString())
}

func getUpdateChangeItem() abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:   abstract.UpdateKind,
		Schema: "my_schema_name",
		Table:  "my_table_name",
		ColumnNames: []string{
			"id",
			"val",
		},
		ColumnValues: []interface{}{
			1,
			2,
		},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), OriginalType: "pg:integer", PrimaryKey: true},
			{ColumnName: "val", DataType: ytschema.TypeInt32.String(), OriginalType: "pg:integer"},
		}),
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{"id"},
			KeyTypes:  nil,
			KeyValues: []interface{}{1},
		},
	}
}

func getUpdateMinBinlogChangeItem() abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:   abstract.UpdateKind,
		Schema: "my_schema_name",
		Table:  "my_table_name",
		ColumnNames: []string{
			"id",
			"val2",
		},
		ColumnValues: []interface{}{
			int32(1),
			int32(2),
		},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), OriginalType: "mysql:int", PrimaryKey: true},
			{ColumnName: "va1l", DataType: ytschema.TypeString.String(), OriginalType: "mysql:text"},
			{ColumnName: "val2", DataType: ytschema.TypeInt32.String(), OriginalType: "mysql:int"},
		}),
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{"id"},
			KeyTypes:  nil,
			KeyValues: []interface{}{int32(1)},
		},
	}
}

func makeChainConversion(t *testing.T, changeItems []abstract.ChangeItem, paramsMap map[string]string) ([]abstract.ChangeItem, []debeziumcommon.KeyValue) {
	params := debeziumparameters.EnrichedWithDefaults(paramsMap)
	emitter, _ := NewMessagesEmitter(params, "", false, logger.Log)
	receiver := NewReceiver(nil, nil)

	result := make([]abstract.ChangeItem, 0)
	kvArr := make([]debeziumcommon.KeyValue, 0)
	for _, changeItem := range changeItems {
		currDebeziumKV, err := emitter.EmitKV(&changeItem, time.Time{}, true, nil)
		require.NoError(t, err)
		kvArr = append(kvArr, currDebeziumKV...)
		recoveredChangeItem, err := receiver.Receive(*currDebeziumKV[0].DebeziumVal)
		require.NoError(t, err)
		result = append(result, *recoveredChangeItem)
	}
	return result, kvArr
}

func TestUpdate(t *testing.T) {
	changeItem := getUpdateChangeItem()
	recoveredChangeItems, _ := makeChainConversion(t, []abstract.ChangeItem{changeItem}, map[string]string{
		debeziumparameters.DatabaseDBName: "public",
		debeziumparameters.TopicPrefix:    "my_topic",
		debeziumparameters.SourceType:     "pg",
	})
	require.Equal(t, 1, len(recoveredChangeItems))

	// check

	require.Equal(t, abstract.UpdateKind, recoveredChangeItems[0].Kind)
	require.False(t, recoveredChangeItems[0].KeysChanged())
	require.NotNil(t, recoveredChangeItems[0].OldKeys)
	require.Equal(t, 1, len(recoveredChangeItems[0].OldKeys.KeyNames))
	require.Equal(t, 1, len(recoveredChangeItems[0].OldKeys.KeyValues))
	require.Equal(t, 2, len(recoveredChangeItems[0].ColumnNames))
	require.Equal(t, 2, len(recoveredChangeItems[0].ColumnValues))
	require.Equal(t, 2, len(recoveredChangeItems[0].TableSchema.Columns()))
}

func TestUpdateTOAST(t *testing.T) {
	changeItem := getUpdateMinBinlogChangeItem()
	recoveredChangeItems, _ := makeChainConversion(t, []abstract.ChangeItem{changeItem}, map[string]string{
		debeziumparameters.DatabaseDBName: "public",
		debeziumparameters.TopicPrefix:    "my_topic",
		debeziumparameters.SourceType:     "mysql",
	})
	require.Equal(t, 1, len(recoveredChangeItems))
}
