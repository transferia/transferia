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
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "mysql",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(setIgnoreUnknownSources)
	currDebeziumKV, err := emitter.EmitKV(originalChangeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))
	return currDebeziumKV
}

func runTwoConversions(t *testing.T, mysqlSnapshotChangeItem []byte, isWipeOriginalTypeInfo bool) (string, string) {
	originalChangeItem, err := abstract.UnmarshalChangeItem(mysqlSnapshotChangeItem)
	require.NoError(t, err)

	currDebeziumKV := emit(t, originalChangeItem, false)

	originalTypes := map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo{
		{Namespace: "", Name: "customers3"}: {"json_": {OriginalType: "mysql:json"}},
	}
	receiver := debezium.NewReceiver(originalTypes, nil)
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

	finalChangeItem, err := receiver.Receive(*finalDebeziumKV[0].DebeziumVal)
	require.NoError(t, err)
	fmt.Printf("final changeItem dump (without original_types info): %s\n", finalChangeItem.ToJSONString())

	return resultRecovered, finalChangeItem.ToJSONString() + "\n"
}

// This test checks - after chain of conversions we get canonized changeItem
//
// - changeItem(with original_type_info) ->
// - debeziumMsg ->
// - changeItem(with original_type_info) ->
// - wipe originalType info ->
// - changeItem(without original_type_info) ->
// - debeziumMsg ->
// - changeItem

func TestEmitterCommonWithWipe(t *testing.T) {
	pgSnapshotChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_chain_test__canon_change_item_original.txt"))
	require.NoError(t, err)

	canonizedRecoveredChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_chain_test__canon_change_item_recovered.txt"))
	require.NoError(t, err)
	canonizedFinalChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_chain_test__canon_change_item_final_wiped.txt"))
	require.NoError(t, err)

	recoveredChangeItemStr, finalChangeItemStr := runTwoConversions(t, pgSnapshotChangeItem, true)

	require.Equal(t, string(canonizedRecoveredChangeItem), recoveredChangeItemStr)
	require.Equal(t, string(canonizedFinalChangeItem), finalChangeItemStr)
}

// This test checks - after chain of conversions we get canonized changeItem
//
// - changeItem(with original_type_info) ->
// - debeziumMsg ->
// - changeItem(with original_type_info) ->
// - debeziumMsg ->
// - changeItem

func TestEmitterCommonWithoutWipe(t *testing.T) {
	mysqlSnapshotChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_chain_test__canon_change_item_original.txt"))
	require.NoError(t, err)

	canonizedRecoveredChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_chain_test__canon_change_item_recovered.txt"))
	require.NoError(t, err)
	canonizedFinalChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_chain_test__canon_change_item_final_not_wiped.txt"))
	require.NoError(t, err)

	recoveredChangeItemStr, finalChangeItemStr := runTwoConversions(t, mysqlSnapshotChangeItem, false)

	require.Equal(t, string(canonizedRecoveredChangeItem), recoveredChangeItemStr)
	require.Equal(t, string(canonizedFinalChangeItem), finalChangeItemStr)
}

func TestEmitterCommonWithoutWipeV8(t *testing.T) {
	mysqlSnapshotChangeItemV8, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_chain_test__canon_change_item_original_v8.txt"))
	require.NoError(t, err)
	_, _ = runTwoConversions(t, mysqlSnapshotChangeItemV8, false)
}
