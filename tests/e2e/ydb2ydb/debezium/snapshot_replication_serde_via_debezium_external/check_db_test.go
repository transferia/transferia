package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/serde"
	simple_transformer "github.com/transferia/transferia/tests/helpers/transformer"
)

var path = "dectest/test-src"
var pathOut = "dectest/test-dst"

func TestSnapshotAndReplicationSerDeViaDebeziumExternal(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{path},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		UseFullPaths:       true,
		ServiceAccountID:   "",
		ChangeFeedMode:     ydb.ChangeFeedModeNewImage,
	}

	Target := &ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	currChangeItem := helpers.YDBInitChangeItem(path)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))

	dst := &ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	helpers.InitSrcDst("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "false",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	originalTypes := map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo{
		{Namespace: "", Name: pathOut}: {
			"id":            {OriginalType: "ydb:Uint64"},
			"Bool_":         {OriginalType: "ydb:Bool"},
			"Int8_":         {OriginalType: "ydb:Int8"},
			"Int16_":        {OriginalType: "ydb:Int16"},
			"Int32_":        {OriginalType: "ydb:Int32"},
			"Int64_":        {OriginalType: "ydb:Int64"},
			"Uint8_":        {OriginalType: "ydb:Uint8"},
			"Uint16_":       {OriginalType: "ydb:Uint16"},
			"Uint32_":       {OriginalType: "ydb:Uint32"},
			"Uint64_":       {OriginalType: "ydb:Uint64"},
			"Float_":        {OriginalType: "ydb:Float"},
			"Double_":       {OriginalType: "ydb:Double"},
			"Decimal_":      {OriginalType: "ydb:Decimal"},
			"DyNumber_":     {OriginalType: "ydb:DyNumber"},
			"String_":       {OriginalType: "ydb:String"},
			"Utf8_":         {OriginalType: "ydb:Utf8"},
			"Json_":         {OriginalType: "ydb:Json"},
			"JsonDocument_": {OriginalType: "ydb:JsonDocument"},
			"Uuid_":         {OriginalType: "ydb:Uuid"},
			"Date_":         {OriginalType: "ydb:Date"},
			"Datetime_":     {OriginalType: "ydb:Datetime"},
			"Timestamp_":    {OriginalType: "ydb:Timestamp"},
			"Interval_":     {OriginalType: "ydb:Interval"},
		},
	}
	receiver := debezium.NewReceiver(originalTypes, nil)
	debeziumSerDeTransformer := simple_transformer.NewSimpleTransformer(t, serde.MakeYdb2YdbDebeziumSerDeUdf(pathOut, nil, emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))

	worker := helpers.Activate(t, transfer)

	//-----------------------------------------------------------------------------------------------------------------
	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsertValues(t, path, helpers.YDBTestValues1, 2),
		*helpers.YDBStmtInsertValues(t, path, helpers.YDBTestValues2, 3),
		*helpers.YDBStmtInsertValues(t, path, helpers.YDBTestValues3, 4),
	}))
	require.NoError(t, helpers.WaitEqualRowsCountDifferentTables(t, "", path, "", pathOut, helpers.GetSampleableStorageByModel(t, src), helpers.GetSampleableStorageByModel(t, dst), 60*time.Second))
	worker.Close(t)
	helpers.YDBTwoTablesEqual(t,
		os.Getenv("YDB_TOKEN"),
		helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		path, pathOut)
}
