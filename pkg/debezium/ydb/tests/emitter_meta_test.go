package tests

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestYDBSourceTxID(t *testing.T) {
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: ytschema.TypeUint64.String(), PrimaryKey: true, OriginalType: "ydb:Uint64"},
	})

	emitSourceTxID := func(t *testing.T, txID string) (interface{}, bool) {
		changeItem := &abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Table:        "test",
			ColumnNames:  []string{"id"},
			ColumnValues: []interface{}{uint64(1)},
			TableSchema:  tableSchema,
			TxID:         txID,
		}
		params := map[string]string{
			debezium_parameters.DatabaseDBName:              "public",
			debezium_parameters.TopicPrefix:                 "my_topic",
			debezium_parameters.SourceType:                  "ydb",
			debezium_parameters.ValueConverterSchemasEnable: "false",
		}
		emitter, err := debezium.NewMessagesEmitter(params, "1.1.2.Final", false, logger.Log)
		require.NoError(t, err)
		kvs, err := emitter.EmitKV(changeItem, time.Time{}, false, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(kvs))
		require.NotNil(t, kvs[0].DebeziumVal)

		var msg struct {
			Source map[string]interface{} `json:"source"`
		}
		require.NoError(t, json.Unmarshal([]byte(*kvs[0].DebeziumVal), &msg))
		txIDVal, ok := msg.Source["txId"]
		return txIDVal, ok
	}

	t.Run("WithVirtualTimestamps", func(t *testing.T) {
		txIDVal, ok := emitSourceTxID(t, "42")
		require.True(t, ok, "txId field must be present in debezium source for ydb")
		require.Equal(t, "42", txIDVal)
	})

	t.Run("WithoutVirtualTimestamps", func(t *testing.T) {
		txIDVal, ok := emitSourceTxID(t, "")
		require.True(t, ok, "txId field must be present in debezium source for ydb")
		require.Nil(t, txIDVal, "txId must be null when ChangeItem.TxID is empty")
	})
}
