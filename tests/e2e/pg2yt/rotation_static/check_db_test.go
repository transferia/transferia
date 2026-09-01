package rotationstatic

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers"
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
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public." + tableName},
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e").(*provider_yt.YtDestinationWrapper)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Target.Model.Static = true
	Target.Model.Cleanup = model.DisabledCleanup
	Target.Model.Rotation = &model.RotatorConfig{
		KeepPartCount:     5,
		PartType:          model.RotatorPartDay,
		PartSize:          1,
		TimeColumn:        "ts",
		TableNameTemplate: "",
	}
}

func TestMain(m *testing.M) {
	provider_yt.InitExe()
	os.Exit(m.Run())
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

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", tableName+"/2026-07-17",
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 2))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", tableName+"/2026-07-15",
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 3))
}
