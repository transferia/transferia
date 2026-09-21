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
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/serde"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_transformer "github.com/transferia/transferia/tests/helpers/transformer"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
)

var path = "dectest/timmyb32r-test"
var pathOut = "dectest/timmyb32r-test-out"
var sourceChangeItem abstract.ChangeItem

func TestSnapshotAndSerDeViaDebeziumEmbedded(t *testing.T) {
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

	t.Run("init source database", func(t *testing.T) {
		Target := &provider_ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		currChangeItem := testdata.YDBInitChangeItem(path)
		for i := 1; i < len(currChangeItem.ColumnValues); i++ {
			currChangeItem.ColumnValues[i] = nil
		}
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))
	})

	dst := &provider_ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	dst.WithDefaults()
	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debezium_parameters.DatabaseDBName:   "public",
		debezium_parameters.TopicPrefix:      "my_topic",
		debezium_parameters.AddOriginalTypes: "true",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)
	debeziumSerDeTransformer := helpers_transformer.NewSimpleTransformer(t, serde.MakeYdb2YdbDebeziumSerDeUdf(pathOut, &sourceChangeItem, emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))

	t.Run("activate", func(t *testing.T) {
		delivery.Activate(t, transfer)
	})

	//-----------------------------------------------------------------------------------------------------------------
	// check

	sinkMock := mocksink.NewMockSink(nil)
	targetMock := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       model.DisabledCleanup,
	}
	transferMock := transferhelpers.MakeTransfer("fake", src, &targetMock, abstract.TransferTypeSnapshotOnly)
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
		delivery.Activate(t, transferMock)
	})
	sourceKeys := sourceChangeItem.KeysAsMap()
	for i := 0; i < len(sourceChangeItem.ColumnValues); i++ {
		if _, ok := sourceKeys[sourceChangeItem.ColumnNames[i]]; ok {
			continue
		}
		require.Nil(t, sourceChangeItem.ColumnValues[i])
	}
	sourceChangeItem.CommitTime = 0
	sourceChangeItem.Table = "!"
	sourceChangeItem.PartID = ""
	sourceChangeItemStr := sourceChangeItem.ToJSONString()
	logger.Log.Infof("sourceChangeItemStr:%s\n", sourceChangeItemStr)

	extractedKeys := extractedChangeItem.KeysAsMap()
	for i := 0; i < len(extractedChangeItem.ColumnValues); i++ {
		if _, ok := extractedKeys[extractedChangeItem.ColumnNames[i]]; ok {
			continue
		}
		require.Nil(t, extractedChangeItem.ColumnValues[i])
	}

	extractedChangeItem.CommitTime = 0
	extractedChangeItem.Table = "!"
	extractedChangeItem.PartID = ""
	extractedChangeItemStr := extractedChangeItem.ToJSONString()
	logger.Log.Infof("extractedChangeItemStr:%s\n", extractedChangeItemStr)
	require.Equal(t, sourceChangeItemStr, extractedChangeItemStr)
}
