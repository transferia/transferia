package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb/recipe"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func customYDBInsertItem(t *testing.T, tablePath string, id int) *abstract.ChangeItem {
	res := testdata.YDBStmtInsert(t, tablePath, id)
	res.TableSchema = abstract.NewTableSchema(append(res.TableSchema.Columns(),
		abstract.ColSchema{PrimaryKey: false, Required: false, ColumnName: "brand_new_text_column", DataType: string(ytschema.TypeString), OriginalType: "ydb:Utf8"},
	))
	res.ColumnNames = append(res.ColumnNames, "brand_new_text_column")
	res.ColumnValues = append(res.ColumnValues, "POOOWEEEER")
	return res
}

func TestSnapshotAndReplication(t *testing.T) {
	for testName, changeFeedMode := range map[string]provider_ydb.ChangeFeedModeType{
		"ModeUpdate":      provider_ydb.ChangeFeedModeUpdates,
		"ModeNewImage":    provider_ydb.ChangeFeedModeNewImage,
		"ModeOldNewImage": provider_ydb.ChangeFeedModeNewAndOldImages,
	} {
		t.Run(testName, func(t *testing.T) {
			testSnapshotAndReplicationWithChangeFeedMode(t, testName, changeFeedMode)
		})
	}
}

func testSnapshotAndReplicationWithChangeFeedMode(t *testing.T, tableName string, mode provider_ydb.ChangeFeedModeType) {
	currTableName := fmt.Sprintf("test_table_%v", tableName)

	source := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{currTableName},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		ChangeFeedMode:     mode,
	}
	target := clickhouse_model.ChDestination{
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "database",
		HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             model.Drop,
	}
	transferType := abstract.TransferTypeSnapshotAndIncrement
	transferhelpers.InitSrcDst(transferhelpers.TransferID, source, &target, transferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

	//---

	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			network.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()

	//---

	Target := &provider_ydb.YdbDestination{
		Database: source.Database,
		Token:    source.Token,
		Instance: source.Instance,
	}
	Target.WithDefaults()
	srcSink, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// insert one rec - for snapshot uploading

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtInsert(t, currTableName, 1),
		*testdata.YDBStmtInsertNulls(t, currTableName, 2),
	}))

	// start snapshot & replication

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, &target, transferType)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	storagecomparison.CheckRowsCount(t, target, target.Database, currTableName, 2)

	// insert two more records - it's three of them now

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtInsertNulls(t, currTableName, 3),
		*testdata.YDBStmtInsert(t, currTableName, 4),
	}))

	if mode == provider_ydb.ChangeFeedModeNewImage || mode == provider_ydb.ChangeFeedModeNewAndOldImages {
		ydbConn := ydbrecipe.Driver(t)
		err = ydbConn.Table().Do(context.Background(), func(ctx context.Context, session ydb_table.Session) (err error) {
			return session.ExecuteSchemeQuery(ctx, fmt.Sprintf(`
--!syntax_v1
ALTER TABLE %s ADD COLUMN brand_new_text_column Text;
`, currTableName))
		})
		require.NoError(t, err)

		err = ydbConn.Table().Do(context.Background(), func(ctx context.Context, session ydb_table.Session) (err error) {
			writeTx := ydb_table.TxControl(
				ydb_table.BeginTx(
					ydb_table.WithSerializableReadWrite(),
				),
				ydb_table.CommitTx(),
			)

			_, _, err = session.Execute(ctx, writeTx, fmt.Sprintf(`
	--!syntax_v1
	UPDATE %s SET brand_new_text_column = 'abc';
	`, currTableName), nil)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)

		// insert another two more records - it's five of them now

		require.NoError(t, srcSink.Push([]abstract.ChangeItem{
			*customYDBInsertItem(t, currTableName, 5),
			*customYDBInsertItem(t, currTableName, 6),
		}))
	}

	// update 2nd rec

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtUpdate(t, currTableName, 4, 666),
	}))

	// update 3rd rec by TOAST

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtUpdateTOAST(t, currTableName, 4, 777),
	}))

	// delete 1st rec

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtDelete(t, currTableName, 1),
	}))

	// check

	if mode == provider_ydb.ChangeFeedModeNewImage || mode == provider_ydb.ChangeFeedModeNewAndOldImages {
		require.NoError(t, storage.WaitDestinationEqualRowsCount(target.Database, currTableName, storagecomparison.GetSampleableStorageByModel(t, target), 60*time.Second, 5))
	} else {
		require.NoError(t, storage.WaitDestinationEqualRowsCount(target.Database, currTableName, storagecomparison.GetSampleableStorageByModel(t, target), 60*time.Second, 3))
	}
}
