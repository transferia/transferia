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
	"github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
)

const tableName = "__test"

var (
	Source = postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{tableName},
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e").(*yt_provider.YtDestinationWrapper)
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
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
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
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// Do some action during replication

	ctx := context.Background()
	srcConn, err := postgres.MakeConnPoolFromSrc(&Source, logger.Log)
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
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", tableName+curTime.Format(format),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 2))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", tableName+curTime.AddDate(0, 0, -2).Format(format),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 2))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", tableName+curTime.AddDate(0, 0, -3).Format(format),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 0))
}
