package tests

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
)

func getNormalized(t *testing.T, in string) string {
	changeItem, err := abstract.UnmarshalChangeItem([]byte(in))
	require.NoError(t, err)
	changeItem.CommitTime = 0
	for i := range changeItem.TableSchema.Columns() {
		if changeItem.TableSchema.Columns()[i].ColumnName == "DyNumber_" {
			changeItem.TableSchema.Columns()[i].DataType = "double"
		}
	}
	str := changeItem.ToJSONString()
	str = strings.ReplaceAll(str, `".123e3"`, `123`)
	return str
}

func TestEmitAndReceive(t *testing.T) {
	changeItemStr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/ydb/tests/testdata/emitter_vals_test__canon_change_item.txt"))
	require.NoError(t, err)
	changeItem, err := abstract.UnmarshalChangeItem(changeItemStr)
	require.NoError(t, err)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)

	resultKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)

	fmt.Println(*resultKV[0].DebeziumVal)

	receiver := debezium.NewReceiver(nil, nil)
	finalChangeItem, err := receiver.Receive(*resultKV[0].DebeziumVal)
	require.NoError(t, err)

	//--------------------------
	// check

	srcNormalized := getNormalized(t, string(changeItemStr))
	dstNormalized := getNormalized(t, finalChangeItem.ToJSONString())
	require.Equal(t, srcNormalized, dstNormalized)
}
