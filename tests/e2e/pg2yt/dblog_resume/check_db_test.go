package dblogresume

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/dblog"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	srcPort = helpers.GetIntFromEnv("PG_LOCAL_PORT")
	Source  = provider_postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      srcPort,
	}
	Target = provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:          "//home/cdc/test/pg2yt_e2e",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		Cleanup:       model.Drop,
	})
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestDBLogResumeWithExistingSlot(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path(Target.Path()), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path(Target.Path()), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	transferID := helpers.GenerateTransferID(t.Name())
	transfer := helpers.MakeTransfer(transferID, &Source, Target, abstract.TransferTypeSnapshotOnly)
	_ = helpers.Activate(t, transfer)

	// Set up the test table and initial data
	srcPool, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcPool.Close()

	_, err = srcPool.Exec(ctx, "truncate table test_table")
	require.NoError(t, err)
	_, err = srcPool.Exec(ctx, "insert into test_table (data) select 'row after the resume' from generate_series(1, 10);")
	require.NoError(t, err)

	// Manually create replication slot with name matching transfer ID
	_, err = srcPool.Exec(ctx, fmt.Sprintf(`SELECT pg_create_logical_replication_slot('%s', 'wal2json')`, transferID))
	require.NoError(t, err)

	// Manually create and populate signal table with a checkpoint not from the beginning
	keeperSchema := "public"
	signalTable, err := dblog.NewPgSignalTable(ctx, srcPool, logger.Log, transferID, keeperSchema)
	require.NoError(t, err)

	// Insert a watermark that represents having processed the first row
	// This means on resume, it should start from the second row
	tableID := abstract.NewTableID("public", "test_table")
	lowBound := []string{"10"}                                         // Start after the 10th row
	_, err = signalTable.CreateWatermark(ctx, *tableID, "S", lowBound) // "S" for success watermark
	require.NoError(t, err)

	// Configure source for DBLog
	Source.DBLogEnabled = true

	// Create transfer with snapshot and increment
	transfer = helpers.MakeTransfer(transferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	t.Logf("starting transfer %s", transfer.ID)
	// Activate transfer
	worker := helpers.Activate(t, transfer)
	defer func() {
		worker.Close(t)
	}()

	t.Logf("waiting for rows count to be 20")
	rows, err := helpers.GetSampleableStorageByModel(t, Target.LegacyModel()).ExactTableRowsCount(*abstract.NewTableID("public", "test_table"))
	require.NoError(t, err)
	require.Equal(t, uint64(20), rows)
	t.Logf("rows count is 20")
}
