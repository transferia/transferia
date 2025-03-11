package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
)

var debeziumSerializerTestTypicalChangeItem *abstract.ChangeItem

func init() {
	var testChangeItem = `{"id":601,"nextlsn":25051056,"commitTime":1643660670333075000,"txPosition":0,"kind":"insert","schema":"public","table":"basic_types15","columnnames":["id","val"],"columnvalues":[1,-8388605],"table_schema":[{"path":"","name":"id","type":"int32","key":true,"required":false,"original_type":"pg:integer","original_type_params":null},{"path":"","name":"val","type":"int32","key":false,"required":false,"original_type":"pg:integer","original_type_params":null}],"oldkeys":{},"tx_id":"","query":""}`
	debeziumSerializerTestTypicalChangeItem, _ = abstract.UnmarshalChangeItem([]byte(testChangeItem))
}

func TestDebeziumSerializerWithDropKeys(t *testing.T) {
	params := map[string]string{
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.TopicPrefix:      defaultTopicPrefix,
		debeziumparameters.SourceType:       "pg",
	}
	changeItem0 := *debeziumSerializerTestTypicalChangeItem
	changeItem0.Table = "table0"

	//-------------------------

	debeziumSerializer, err := NewDebeziumSerializer(params, false, true, false, logger.Log)
	require.NoError(t, err)
	batches, err := debeziumSerializer.Serialize([]abstract.ChangeItem{changeItem0})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	for _, elements := range batches {
		for _, element := range elements {
			require.Equal(t, []byte{}, element.Key)
			require.NotNil(t, element.Value)
		}
	}
}

func TestDebeziumSerializerSnapshot(t *testing.T) {
	params := map[string]string{
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.TopicPrefix:      defaultTopicPrefix,
		debeziumparameters.SourceType:       "pg",
	}
	changeItem0 := *debeziumSerializerTestTypicalChangeItem
	changeItem0.Table = "snapshot"

	//----------------------------------------

	debeziumSerializer, err := NewDebeziumSerializer(params, false, false, true, logger.Log)
	require.NoError(t, err)
	batches, err := debeziumSerializer.Serialize([]abstract.ChangeItem{changeItem0})
	require.NoError(t, err)
	require.Len(t, batches, 1)

	for _, elements := range batches {
		for _, element := range elements {
			require.Equal(t, `{"payload":{"id":1},"schema":{"fields":[{"field":"id","optional":false,"type":"int32"}],"name":"__data_transfer_stub.public.snapshot.Key","optional":false,"type":"struct"}}`, string(element.Key))
			require.Equal(t, `{"payload":{"after":{"id":1,"val":-8388605},"before":null,"op":"r","source":{"connector":"postgresql","db":"","lsn":25051056,"name":"__data_transfer_stub","schema":"public","snapshot":"true","table":"snapshot","ts_ms":1643660670333,"txId":601,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1643660670333},"schema":{"fields":[{"field":"before","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"__data_transfer_stub.public.snapshot.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"__data_transfer_stub.public.snapshot.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"__data_transfer_stub.public.snapshot.Envelope","optional":false,"type":"struct"}}`, string(element.Value))
		}
	}
}

func TestDebeziumSerializerEmptyInput(t *testing.T) {
	debeziumSerializer, err := NewDebeziumSerializer(map[string]string{
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.TopicPrefix:      defaultTopicPrefix,
		debeziumparameters.SourceType:       "pg",
	}, false, false, false, logger.Log)
	require.NoError(t, err)

	batches, err := debeziumSerializer.Serialize([]abstract.ChangeItem{})
	require.NoError(t, err)
	require.Len(t, batches, 0)
}

func TestDebeziumSerializerTopicName(t *testing.T) {
	params := map[string]string{
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.TopicPrefix:      defaultTopicPrefix,
		debeziumparameters.SourceType:       "pg",
	}
	changeItem0 := *debeziumSerializerTestTypicalChangeItem
	changeItem0.Table = "table0"
	changeItem1 := *debeziumSerializerTestTypicalChangeItem
	changeItem1.Table = "table1"

	//-------------------------
	// saveTxOrder: false

	debeziumSerializer, err := NewDebeziumSerializer(params, false, false, false, logger.Log)
	require.NoError(t, err)
	batches, err := debeziumSerializer.Serialize([]abstract.ChangeItem{changeItem0, changeItem1})
	require.NoError(t, err)
	require.Len(t, batches, 2)

	var ok bool
	var v []SerializedMessage

	v, ok = batches[abstract.TablePartID{TableID: abstract.TableID{Namespace: "public", Name: "table0"}, PartID: ""}]
	require.True(t, ok)
	require.Equal(t, `{"payload":{"id":1},"schema":{"fields":[{"field":"id","optional":false,"type":"int32"}],"name":"__data_transfer_stub.public.table0.Key","optional":false,"type":"struct"}}`, string(v[0].Key))
	require.Equal(t, `{"payload":{"after":{"id":1,"val":-8388605},"before":null,"op":"c","source":{"connector":"postgresql","db":"","lsn":25051056,"name":"__data_transfer_stub","schema":"public","snapshot":"false","table":"table0","ts_ms":1643660670333,"txId":601,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1643660670333},"schema":{"fields":[{"field":"before","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"__data_transfer_stub.public.table0.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"__data_transfer_stub.public.table0.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"__data_transfer_stub.public.table0.Envelope","optional":false,"type":"struct"}}`, string(v[0].Value))

	v, ok = batches[abstract.TablePartID{TableID: abstract.TableID{Namespace: "public", Name: "table1"}, PartID: ""}]
	require.True(t, ok)
	require.Equal(t, `{"payload":{"id":1},"schema":{"fields":[{"field":"id","optional":false,"type":"int32"}],"name":"__data_transfer_stub.public.table1.Key","optional":false,"type":"struct"}}`, string(v[0].Key))
	require.Equal(t, `{"payload":{"after":{"id":1,"val":-8388605},"before":null,"op":"c","source":{"connector":"postgresql","db":"","lsn":25051056,"name":"__data_transfer_stub","schema":"public","snapshot":"false","table":"table1","ts_ms":1643660670333,"txId":601,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1643660670333},"schema":{"fields":[{"field":"before","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"__data_transfer_stub.public.table1.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"__data_transfer_stub.public.table1.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"__data_transfer_stub.public.table1.Envelope","optional":false,"type":"struct"}}`, string(v[0].Value))

	//------------------------
	// saveTxOrder: true

	debeziumSerializerTx, err := NewDebeziumSerializer(params, true, false, false, logger.Log)
	require.NoError(t, err)
	batchesTx, err := debeziumSerializerTx.Serialize([]abstract.ChangeItem{changeItem0, changeItem1})
	require.NoError(t, err)
	require.Len(t, batchesTx, 1)
	for tableID, arr := range batchesTx {
		require.Equal(t, abstract.TablePartID{TableID: abstract.TableID{Namespace: "", Name: ""}, PartID: ""}, tableID)
		require.Len(t, arr, 2)
	}
}

func TestDebeziumSerializerTopicPrefix(t *testing.T) {
	debeziumSerializer, err := NewDebeziumSerializer(map[string]string{
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.TopicPrefix:      "my_topic_prefix",
		debeziumparameters.SourceType:       "pg",
	}, false, false, false, logger.Log)
	require.NoError(t, err)

	batches, err := debeziumSerializer.Serialize([]abstract.ChangeItem{*debeziumSerializerTestTypicalChangeItem})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	for k, v := range batches {
		require.Equal(t, debeziumSerializerTestTypicalChangeItem.TablePartID(), k)
		require.Len(t, v, 1)
		require.Equal(t, `{"payload":{"id":1},"schema":{"fields":[{"field":"id","optional":false,"type":"int32"}],"name":"my_topic_prefix.public.basic_types15.Key","optional":false,"type":"struct"}}`, string(v[0].Key))
		require.Equal(t, `{"payload":{"after":{"id":1,"val":-8388605},"before":null,"op":"c","source":{"connector":"postgresql","db":"","lsn":25051056,"name":"my_topic_prefix","schema":"public","snapshot":"false","table":"basic_types15","ts_ms":1643660670333,"txId":601,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1643660670333},"schema":{"fields":[{"field":"before","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"my_topic_prefix.public.basic_types15.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"my_topic_prefix.public.basic_types15.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"my_topic_prefix.public.basic_types15.Envelope","optional":false,"type":"struct"}}`, string(v[0].Value))
	}
}
