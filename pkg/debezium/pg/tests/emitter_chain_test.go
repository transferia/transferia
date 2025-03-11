package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
)

func wipeOriginalTypeInfo(changeItem *abstract.ChangeItem) *abstract.ChangeItem {
	for i := range changeItem.TableSchema.Columns() {
		changeItem.TableSchema.Columns()[i].OriginalType = ""
	}
	return changeItem
}

func emit(t *testing.T, originalChangeItem *abstract.ChangeItem, setIgnoreUnknownSources bool) []debeziumcommon.KeyValue {
	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(setIgnoreUnknownSources)
	currDebeziumKV, err := emitter.EmitKV(originalChangeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))
	return currDebeziumKV
}

func runTwoConversions(t *testing.T, pgSnapshotChangeItem []byte, isWipeOriginalTypeInfo bool) (string, string) {
	originalChangeItem, err := abstract.UnmarshalChangeItem(pgSnapshotChangeItem)
	require.NoError(t, err)

	currDebeziumKV := emit(t, originalChangeItem, false)

	receiver := debezium.NewReceiver(nil, nil)
	recoveredChangeItem, err := receiver.Receive(*currDebeziumKV[0].DebeziumVal)
	require.NoError(t, err)
	resultRecovered := recoveredChangeItem.ToJSONString() + "\n"

	if isWipeOriginalTypeInfo {
		recoveredChangeItem = wipeOriginalTypeInfo(recoveredChangeItem)
		fmt.Printf("recovered changeItem dump (without original_types info): %s\n", recoveredChangeItem.ToJSONString())
	}

	finalDebeziumKV := emit(t, recoveredChangeItem, true)
	require.Equal(t, 1, len(finalDebeziumKV))
	fmt.Printf("final debezium msg: %s\n", *finalDebeziumKV[0].DebeziumVal)

	require.Equal(t, 1, len(finalDebeziumKV))
	fmt.Printf("final debezium msg: %s\n", *finalDebeziumKV[0].DebeziumVal)

	finalChangeItem, err := receiver.Receive(*finalDebeziumKV[0].DebeziumVal)
	require.NoError(t, err)
	fmt.Printf("final changeItem dump (without original_types info): %s\n", finalChangeItem.ToJSONString())

	return resultRecovered, finalChangeItem.ToJSONString() + "\n"
}

// TestEmitterCommonWithWipe tests the following chain of operations against the canonized output:
//
// - changeItem (with original_type_info) ->
// - debeziumMsg ->
// - changeItem (without original_type_info) ->
// - debeziumMsg ->
// - changeItem
func TestEmitterCommonWithWipe(t *testing.T) {
	pgSnapshotChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_chain_test__canon_change_item_original.txt"))
	require.NoError(t, err)

	canonizedRecoveredChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_chain_test__canon_change_item_recovered.txt"))
	require.NoError(t, err)
	canonizedFinalChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_chain_test__canon_change_item_final_wiped.txt"))
	require.NoError(t, err)

	recoveredChangeItemStr, finalChangeItemStr := runTwoConversions(t, pgSnapshotChangeItem, true)

	require.Equal(t, string(canonizedRecoveredChangeItem), recoveredChangeItemStr)
	require.Equal(t, string(canonizedFinalChangeItem), finalChangeItemStr)
}

// TestEmitterCommonWithoutWipe tests the following chain of operations against the canonized output:
//
// - changeItem (with original_type_info) ->
// - debeziumMsg ->
// - changeItem (with original_type_info) ->
// - debeziumMsg ->
// - changeItem
func TestEmitterCommonWithoutWipe(t *testing.T) {
	pgSnapshotChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_chain_test__canon_change_item_original.txt"))
	require.NoError(t, err)

	canonizedRecoveredChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_chain_test__canon_change_item_recovered.txt"))
	require.NoError(t, err)
	canonizedFinalChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_chain_test__canon_change_item_final_not_wiped.txt"))
	require.NoError(t, err)

	recoveredChangeItemStr, finalChangeItemStr := runTwoConversions(t, pgSnapshotChangeItem, false)

	require.Equal(t, string(canonizedRecoveredChangeItem), recoveredChangeItemStr)
	require.Equal(t, string(canonizedFinalChangeItem), finalChangeItemStr)
}
