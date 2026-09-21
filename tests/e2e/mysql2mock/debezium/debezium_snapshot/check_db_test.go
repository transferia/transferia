package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debezium_common "github.com/transferia/transferia/pkg/debezium/common"
	debezium_testutil "github.com/transferia/transferia/pkg/debezium/testutil"
	"github.com/transferia/transferia/tests/helpers/changeitem"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = mysql.RecipeMysqlSource()
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Source.AllowDecimalAsFloat = true
}

//---------------------------------------------------------------------------------------------------------------------

func TestSnapshot(t *testing.T) {
	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "mysql source", Port: Source.Port},
	))

	canonizedDebeziumKeyBytes, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_snapshot/testdata/change_item_key.txt"))
	require.NoError(t, err)
	canonizedDebeziumValBytes, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_snapshot/testdata/change_item_val.txt"))
	require.NoError(t, err)
	canonizedDebeziumVal := string(canonizedDebeziumValBytes)

	//------------------------------------------------------------------------------

	sinker := mocksink.NewMockSink(nil)
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := transferhelpers.MakeTransfer("fake", Source, &target, abstract.TransferTypeSnapshotOnly)

	var changeItems []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		changeItems = append(changeItems, input...)
		return nil
	}

	delivery.Activate(t, transfer)

	require.Equal(t, 5, len(changeItems))
	require.Equal(t, changeItems[0].Kind, abstract.InitShardedTableLoad)
	require.Equal(t, changeItems[1].Kind, abstract.InitTableLoad)
	require.Equal(t, changeItems[2].Kind, abstract.InsertKind)
	require.Equal(t, changeItems[3].Kind, abstract.DoneTableLoad)
	require.Equal(t, changeItems[4].Kind, abstract.DoneShardedTableLoad)

	fmt.Printf("changeItem dump: %s\n", changeItems[2].ToJSONString())

	debezium_testutil.CheckCanonizedDebeziumEvent(t, &changeItems[2], "dbserver1", "source", "mysql", true, []debezium_common.KeyValue{{DebeziumKey: string(canonizedDebeziumKeyBytes), DebeziumVal: &canonizedDebeziumVal}})

	changeItemBuf, err := json.Marshal(changeItems[2])
	require.NoError(t, err)
	changeItemDeserialized := changeitem.UnmarshalChangeItem(t, changeItemBuf)
	debezium_testutil.CheckCanonizedDebeziumEvent(t, changeItemDeserialized, "dbserver1", "source", "mysql", true, []debezium_common.KeyValue{{DebeziumKey: string(canonizedDebeziumKeyBytes), DebeziumVal: &canonizedDebeziumVal}})
}
