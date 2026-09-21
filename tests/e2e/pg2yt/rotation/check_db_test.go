package rotation

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
)

const tableName = "__test"

var (
	Source = provider_postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      testenv.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{tableName},
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e").(*provider_yt.YtDestinationWrapper)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Target.Model.Rotation = &model.RotatorConfig{
		KeepPartCount:     5,
		PartType:          model.RotatorPartDay,
		PartSize:          1,
		TimeColumn:        "ts",
		TableNameTemplate: "",
	}
}

func TestGroup(t *testing.T) {
	targetPort, err := network.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()
	Source.PreSteps.Constraint = true
	t.Setenv("TZ", "Europe/Moscow")

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("SnapshotAndIncrement", SnapshotAndIncrement)
	})
}

func SnapshotAndIncrement(t *testing.T) {
	// Make transfer and do snapshot
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	// Do some action during replication

	ctx := context.Background()
	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	_, err = srcConn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, ts, astr) VALUES (4, now(), 'astr4');", tableName))
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, fmt.Sprintf("UPDATE %s SET ts = (now() - INTERVAL '2 DAYS') WHERE id = 1;", tableName))
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 3;", tableName))
	require.NoError(t, err)

	// Check storage

	curTime := time.Now()
	format := "/2006-01-02"
	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", tableName+curTime.Format(format),
		storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 2))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", tableName+curTime.AddDate(0, 0, -2).Format(format),
		storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 2))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", tableName+curTime.AddDate(0, 0, -3).Format(format),
		storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 0))
}
