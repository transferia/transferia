package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/debezium"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	"github.com/transferia/transferia/tests/helpers/serde"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_transformer "github.com/transferia/transferia/tests/helpers/transformer"
	"github.com/transferia/transferia/tests/helpers/ydb"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
)

var path = "dectest/test-src"
var pathOut = "dectest/test-dst"

func TestSnapshotAndReplicationSerDeViaDebeziumNotEnriched(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{path},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		UseFullPaths:       true,
		ServiceAccountID:   "",
		ChangeFeedMode:     provider_ydb.ChangeFeedModeNewImage,
	}

	Target := &provider_ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	currChangeItem := testdata.YDBInitChangeItem(path)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))

	dst := &provider_ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	transferhelpers.InitSrcDst("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debezium_parameters.DatabaseDBName:   "public",
		debezium_parameters.TopicPrefix:      "my_topic",
		debezium_parameters.AddOriginalTypes: "false",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)
	debeziumSerDeTransformer := helpers_transformer.NewSimpleTransformer(t, serde.MakeYdb2YdbDebeziumSerDeUdf(pathOut, nil, emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))

	worker := delivery.Activate(t, transfer)

	//-----------------------------------------------------------------------------------------------------------------
	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*testdata.YDBStmtInsertValues(t, path, testdata.YDBTestValues1, 2),
		*testdata.YDBStmtInsertNulls(t, path, 3),
		*testdata.YDBStmtInsertValues(t, path, testdata.YDBTestValues3, 4),
	}))
	require.NoError(t, storage.WaitEqualRowsCountDifferentTables(t, "", path, "", pathOut, storagecomparison.GetSampleableStorageByModel(t, src), storagecomparison.GetSampleableStorageByModel(t, dst), 60*time.Second))
	worker.Close(t)

	dump := ydb.PullDataFromTable(t,
		os.Getenv("YDB_TOKEN"),
		testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		pathOut)
	for i := 0; i < len(dump); i++ {
		dump[i].CommitTime = 0
		dump[i].PartID = ""
	}
	canon.SaveJSON(t, dump)
}
