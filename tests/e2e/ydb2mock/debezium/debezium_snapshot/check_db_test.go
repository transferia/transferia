package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debezium_common "github.com/transferia/transferia/pkg/debezium/common"
	debezium_testutil "github.com/transferia/transferia/pkg/debezium/testutil"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/changeitem"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
)

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	//-----------------------------------------------------------------------------------------------------------------

	canonizedDebeziumKeyArr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/ydb2mock/debezium/debezium_snapshot/testdata/change_item_key.txt"))
	require.NoError(t, err)
	canonizedDebeziumValArr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/ydb2mock/debezium/debezium_snapshot/testdata/change_item_val.txt"))
	require.NoError(t, err)
	canonizedDebeziumVal := string(canonizedDebeziumValArr)

	//-----------------------------------------------------------------------------------------------------------------
	// init

	t.Run("init source database", func(t *testing.T) {
		Target := &provider_ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		currChangeItem := testdata.YDBInitChangeItem("dectest/timmyb32r-test")
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))
	})

	//-----------------------------------------------------------------------------------------------------------------
	// activate

	sinker := mocksink.NewMockSink(nil)
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := transferhelpers.MakeTransfer("fake", src, &target, abstract.TransferTypeSnapshotOnly)

	var changeItems []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		changeItems = append(changeItems, input...)
		return nil
	}

	delivery.Activate(t, transfer)

	//-----------------------------------------------------------------------------------------------------------------
	// check

	require.Equal(t, 5, len(changeItems))
	require.Equal(t, changeItems[0].Kind, abstract.InitShardedTableLoad)
	require.Equal(t, changeItems[1].Kind, abstract.InitTableLoad)
	require.Equal(t, changeItems[2].Kind, abstract.InsertKind)
	require.Equal(t, changeItems[3].Kind, abstract.DoneTableLoad)
	require.Equal(t, changeItems[4].Kind, abstract.DoneShardedTableLoad)

	logger.Log.Infof("changeItem dump: %s\n", changeItems[2].ToJSONString())

	debezium_testutil.CheckCanonizedDebeziumEvent(t, &changeItems[2], "fullfillment", "pguser", "pg", true, []debezium_common.KeyValue{{DebeziumKey: string(canonizedDebeziumKeyArr), DebeziumVal: &canonizedDebeziumVal}})
	changeItemBuf, err := json.Marshal(changeItems[2])
	require.NoError(t, err)
	changeItemDeserialized := changeitem.UnmarshalChangeItem(t, changeItemBuf)
	debezium_testutil.CheckCanonizedDebeziumEvent(t, changeItemDeserialized, "fullfillment", "pguser", "pg", true, []debezium_common.KeyValue{{DebeziumKey: string(canonizedDebeziumKeyArr), DebeziumVal: &canonizedDebeziumVal}})
}
