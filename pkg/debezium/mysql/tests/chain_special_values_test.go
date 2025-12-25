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
)

func TestUint64(t *testing.T) {
	changeItemStr := `{"id":0,"nextlsn":0,"commitTime":0,"txPosition":0,"kind":"insert","schema":"","table":"customers3","part":"","columnnames":["pk","bigint_u"],"columnvalues":[2,18446744073709551615],"table_schema":[{"table_schema":"","table_name":"customers3","path":"","name":"pk","type":"uint32","key":true,"fake_key":false,"required":false,"expression":"","original_type":"mysql:int(10) unsigned"},{"table_schema":"","table_name":"customers3","path":"","name":"bigint_u","type":"uint64","key":false,"fake_key":false,"required":false,"expression":"","original_type":"mysql:bigint(20) unsigned"}],"oldkeys":{},"tx_id":"","query":""}`
	changeItem, err := abstract.UnmarshalChangeItem([]byte(changeItemStr))
	require.NoError(t, err)

	// check if conversation works

	params := map[string]string{
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "mysql",
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
