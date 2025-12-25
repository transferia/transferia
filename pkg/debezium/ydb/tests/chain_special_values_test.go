package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestUint64(t *testing.T) {
	changeItem := &abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "bigint_u"},
		ColumnValues: []interface{}{uint64(1), uint64(18446744073709551615)},
		TableSchema: abstract.NewTableSchema(
			[]abstract.ColSchema{
				{ColumnName: "id", DataType: ytschema.TypeUint64.String(), OriginalType: "ydb:Uint64"},
				{ColumnName: "bigint_u", DataType: ytschema.TypeUint64.String(), OriginalType: "ydb:Uint64"},
			},
		),
	}

	// check if conversation works

	params := map[string]string{
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "ydb",
	}
	emitter, err := debezium.NewMessagesEmitter(params, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(true)
	currDebeziumKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))

	receiver := debezium.NewReceiver(nil, nil)
	recoveredChangeItem, err := receiver.Receive(*currDebeziumKV[0].DebeziumVal)
	require.NoError(t, err)

	require.Equal(t, changeItem.ToJSONString(), recoveredChangeItem.ToJSONString())

	// check values

	afterVals, err := debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%T", int64(0)), fmt.Sprintf("%T", afterVals["bigint_u"]))
	require.Equal(t, int64(-1), afterVals["bigint_u"].(int64))

	require.Equal(t, fmt.Sprintf("%T", uint64(0)), fmt.Sprintf("%T", changeItem.AsMap()["bigint_u"]))
	require.Equal(t, uint64(18446744073709551615), changeItem.AsMap()["bigint_u"].(uint64))
	require.Equal(t, fmt.Sprintf("%T", uint64(0)), fmt.Sprintf("%T", recoveredChangeItem.AsMap()["bigint_u"]))
	require.Equal(t, uint64(18446744073709551615), recoveredChangeItem.AsMap()["bigint_u"].(uint64))
}
