package snapshot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	Source = provider_postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      testenv.GetIntFromEnv("PG_LOCAL_PORT"),
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
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

	t.Run("Snapshot", Snapshot)
	t.Run("SnapshotOldVersion", SnapshotOldVersion)
}

func Snapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	_ = delivery.Activate(t, transfer)

	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target.LegacyModel(), storagecomparison.NewCompareStorageParams()))
}

func SnapshotOldVersion(t *testing.T) {
	SourceCopy := Source
	SourceCopy.DBTables = []string{"test_timestamp"}
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &SourceCopy, Target, abstract.TransferTypeSnapshotOnly)
	transfer.TypeSystemVersion = 1

	_ = delivery.Activate(t, transfer)
}
