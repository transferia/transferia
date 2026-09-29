package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/e2e/mysql2ch"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	Source = provider_mysql.MysqlSource{
		Host:                os.Getenv("RECIPE_MYSQL_HOST"),
		User:                os.Getenv("RECIPE_MYSQL_USER"),
		Password:            model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database:            os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:                testenv.GetIntFromEnv("RECIPE_MYSQL_PORT"),
		AllowDecimalAsFloat: true,
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/mysql2yt_e2e_all_datatypes")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestSnapshot(t *testing.T) {
	targetPort, err := network.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "MySQL source", Port: Source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()
	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_all_datatypes"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_all_datatypes"), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	targetForCompare, ok := Target.(*provider_yt.YtDestinationWrapper)
	require.True(t, ok)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	_ = delivery.Activate(t, transfer)
	require.NoError(t, storagecomparison.CompareStorages(t, &Source, targetForCompare, storagecomparison.NewCompareStorageParams().WithPriorityComparators(mysql2ch.MySQLBytesToStringOptionalComparator)))
}
