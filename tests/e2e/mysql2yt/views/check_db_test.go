package snapshot

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	source = provider_mysql.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     testenv.GetIntFromEnv("RECIPE_MYSQL_PORT"),
	}
	target = provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:    "//home/cdc/test/mysql2yt_e2e_snapshot",
		Cluster: os.Getenv("YT_PROXY"),
		Static:  true,
	})
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	source.WithDefaults()
	target.WithDefaults()
}

func TestGroup(t *testing.T) {
	targetPort, err := network.GetPortFromStr(target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_snapshot"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_snapshot"), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
	})
}

func Existence(t *testing.T) {
	storagecomparison.GetSampleableStorageByModel(t, source)
	storagecomparison.GetSampleableStorageByModel(t, target.LegacyModel().(*provider_yt.YtDestination))
}

func Snapshot(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &source, target, abstract.TransferTypeSnapshotOnly)
	_ = delivery.Activate(t, transfer)
	require.NoError(t, storage.WaitEqualRowsCount(t, source.Database, "__test_view", storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, target.LegacyModel()), 10*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, source.Database, "__test", storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, target.LegacyModel()), 10*time.Second))
}
