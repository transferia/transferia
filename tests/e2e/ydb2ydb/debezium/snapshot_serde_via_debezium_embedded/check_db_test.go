package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/serde"
	simple_transformer "github.com/transferia/transferia/tests/helpers/transformer"
)

var path = "dectest/timmyb32r-test"
var pathOut = "dectest/timmyb32r-test-out"
var sourceChangeItem abstract.ChangeItem

func TestSnapshotSerDeViaDebeziumEmbedded(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	t.Run("init source database", func(t *testing.T) {
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
	})

	dst := &ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	dst.WithDefaults()
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)
	debeziumSerDeTransformer := simple_transformer.NewSimpleTransformer(t, serde.MakeYdb2YdbDebeziumSerDeUdf(pathOut, &sourceChangeItem, emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))

	t.Run("activate", func(t *testing.T) {
		helpers.Activate(t, transfer)
	})

	//-----------------------------------------------------------------------------------------------------------------
	// check
	sinkMock := &helpers.MockSink{}
	targetMock := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       model.DisabledCleanup,
	}
	transferMock := helpers.MakeTransfer("fake", src, &targetMock, abstract.TransferTypeSnapshotOnly)
	var extractedChangeItem abstract.ChangeItem
	t.Run("extract change_item from dst", func(t *testing.T) {
		sinkMock.PushCallback = func(input []abstract.ChangeItem) error {
			for _, currItem := range input {
				if currItem.Table == pathOut && currItem.Kind == abstract.InsertKind {
					extractedChangeItem = currItem
				}
			}
			return nil
		}
		helpers.Activate(t, transferMock)
	})

	sourceChangeItem.CommitTime = 0
	sourceChangeItem.Table = "!"
	sourceChangeItem.PartID = ""
	sourceChangeItemStr := sourceChangeItem.ToJSONString()
	logger.Log.Infof("sourceChangeItemStr:%s\n", sourceChangeItemStr)

	extractedChangeItem.CommitTime = 0
	extractedChangeItem.Table = "!"
	extractedChangeItem.PartID = ""
	extractedChangeItemStr := extractedChangeItem.ToJSONString()
	logger.Log.Infof("extractedChangeItemStr:%s\n", extractedChangeItemStr)

	require.Equal(t, sourceChangeItemStr, extractedChangeItemStr)
}
