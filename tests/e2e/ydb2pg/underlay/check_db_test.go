package main

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
)

const pgSchema = "public"

func init() {
	_ = os.Setenv("YC", "1")
}

func TestSnapshotAndReplication(t *testing.T) {
	currTableName := "test_table"

	source := &provider_ydb.YdbSource{
		Token:              "",
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{currTableName},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           true,
		ServiceAccountID:   "",
		ChangeFeedMode:     provider_ydb.ChangeFeedModeNewImage,
	}

	dstPort, err := strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	require.NoError(t, err)
	target := provider_postgres.PgDestination{
		Hosts:     []string{"localhost"},
		ClusterID: os.Getenv("TARGET_CLUSTER_ID"),
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      dstPort,
		Cleanup:   model.Drop,
	}

	transferType := abstract.TransferTypeSnapshotAndIncrement
	transferhelpers.InitSrcDst(transferhelpers.TransferID, source, &target, transferType)

	defer func() {
		ydbPort, perr := network.GetPortFromStr(source.Instance)
		require.NoError(t, perr)
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "YDB source", Port: ydbPort},
			network.LabeledPort{Label: "Pg target", Port: target.Port},
		))
	}()

	ydbSinkDst := &provider_ydb.YdbDestination{
		Database: source.Database,
		Instance: source.Instance,
	}
	ydbSinkDst.WithDefaults()
	srcSink, err := provider_ydb.NewSinker(logger.Log, ydbSinkDst, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtInsert(t, currTableName, 1),
		*testdata.YDBStmtInsertNulls(t, currTableName, 2),
		*testdata.YDBStmtInsertNulls(t, currTableName, 3),
		*testdata.YDBStmtInsert(t, currTableName, 4),
	}))

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, &target, transferType)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, storage.WaitEqualRowsCountDifferentTables(
		t,
		"", currTableName,
		pgSchema, currTableName,
		storagecomparison.GetSampleableStorageByModel(t, source),
		storagecomparison.GetSampleableStorageByModel(t, &target),
		60*time.Second,
	))
}
