package main

import (
	"context"
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
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

var path = "dectest/timmyb32r-test"
var pathOut = "dectest/timmyb32r-test-out"
var sourceChangeItem abstract.ChangeItem

func TestSnapshotSerDeViaDebeziumEmbeddedOLAP(t *testing.T) {
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
		Token:                 model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:              helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:              helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		IsTableColumnOriented: true,
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
	var foundInOlap uint8
	t.Run("Check by selfclient", func(t *testing.T) {
		clientCtx, cancelFunc := context.WithCancel(context.Background())
		url := "grpc://" + helpers.GetEnvOfFail(t, "YDB_ENDPOINT") + "/" + helpers.GetEnvOfFail(t, "YDB_DATABASE")
		db, err := ydbsdk.Open(clientCtx, url)
		require.NoError(t, err)

		require.NoError(t, db.Table().Do(clientCtx, func(clientCtx context.Context, s table.Session) (err error) {
			query := "SELECT COUNT(*) as co, MAX(`Bool_`) as bo FROM `dectest/timmyb32r-test-out`;"
			res, err := s.StreamExecuteScanQuery(clientCtx, query, nil)
			if err != nil {
				logger.Log.Infof("cant execute")
				return err
			}
			defer res.Close()
			if err = res.NextResultSetErr(clientCtx); err != nil {
				logger.Log.Infof("no resultset")
				return err
			}
			var count uint64
			for res.NextRow() {
				err = res.ScanNamed(named.Required("co", &count), named.Required("bo", &foundInOlap))
			}
			require.Equal(t, uint64(1), count)
			return res.Err()
		}))
		cancelFunc()
	})
	sourceBool := uint8(0)
	if sourceChangeItem.ColumnValues[1].(bool) {
		sourceBool = uint8(1)
	}
	require.Equal(t, sourceBool, foundInOlap)
}
