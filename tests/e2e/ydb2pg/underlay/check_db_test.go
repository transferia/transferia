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
	"github.com/transferia/transferia/tests/helpers"
)

const pgSchema = "public"

func init() {
	_ = os.Setenv("YC", "1")
}

func TestSnapshotAndReplication(t *testing.T) {
	currTableName := "test_table"

	source := &provider_ydb.YdbSource{
		Token:              "",
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
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
	helpers.InitSrcDst(helpers.TransferID, source, &target, transferType)

	defer func() {
		ydbPort, perr := helpers.GetPortFromStr(source.Instance)
		require.NoError(t, perr)
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YDB source", Port: ydbPort},
			helpers.LabeledPort{Label: "Pg target", Port: target.Port},
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
		*helpers.YDBStmtInsert(t, currTableName, 1),
		*helpers.YDBStmtInsertNulls(t, currTableName, 2),
		*helpers.YDBStmtInsertNulls(t, currTableName, 3),
		*helpers.YDBStmtInsert(t, currTableName, 4),
	}))

	transfer := helpers.MakeTransfer(helpers.TransferID, source, &target, transferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, helpers.WaitEqualRowsCountDifferentTables(
		t,
		"", currTableName,
		pgSchema, currTableName,
		helpers.GetSampleableStorageByModel(t, source),
		helpers.GetSampleableStorageByModel(t, &target),
		60*time.Second,
	))
}
