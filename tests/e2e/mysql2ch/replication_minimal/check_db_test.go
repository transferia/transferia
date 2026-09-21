package snapshot

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName = "source"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = provider_mysql.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     testenv.GetIntFromEnv("RECIPE_MYSQL_PORT"),
		ServerID: 1, // what is it?
	}
	Target = clickhouse_model.ChDestination{
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
		Database:            databaseName,
		HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestReplication(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: Source.Port},
			network.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	connParams, err := provider_mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)

	client, err := provider_mysql.Connect(connParams, nil)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	fakeClient := coordinator.NewStatefulFakeClient()
	err = provider_mysql.SyncBinlogPosition(&Source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// insert/update/delete several record

	rows, err := client.Query("INSERT INTO __test (id, val1, val2) VALUES (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e')")
	require.NoError(t, err)
	_ = rows.Close()

	rows, err = client.Query("UPDATE __test SET val2='ee' WHERE id=5;")
	require.NoError(t, err)
	_ = rows.Close()

	rows, err = client.Query("DELETE FROM __test WHERE id=3;")
	require.NoError(t, err)
	_ = rows.Close()

	//------------------------------------------------------------------------------------
	// wait & compare

	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(compareDataTypes)))
}

func compareDataTypes(l, r string) bool {
	if l == "utf8" && r == "string" {
		return true
	}
	return l == r
}
