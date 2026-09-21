package jsoncanonical

import (
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/storage"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
)

// Test cases

func TestSnapshotAndReplication(t *testing.T) {
	fixture := mysql.SetupMySQL2YTTest(t, makeMysqlSource("test_snapshot_and_increment"), helpers_yt.RecipeYtTarget(string(helpers_yt.YtTestDir(t, "json_canonical"))))
	defer fixture.Teardown(t)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, fixture.Src, fixture.Dst, abstract.TransferTypeSnapshotAndIncrement)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	mysql.ExecuteMySQLStatement(t, snapshotAndIncrementSQL, fixture.SrcStorage.ConnectionParams)

	require.NoError(t, storage.WaitEqualRowsCount(t, fixture.Src.Database, "test_snapshot_and_increment", fixture.SrcStorage, fixture.DstStorage, time.Second*30))
	helpers_yt.CanonizeDynamicYtTable(t, fixture.YTEnv.YT, ypath.Path(fmt.Sprintf("%s/%s_test_snapshot_and_increment", fixture.YTDir, fixture.Src.Database)), "yt_table.yson")
}

func TestReplication(t *testing.T) {
	fixture := mysql.SetupMySQL2YTTest(t, makeMysqlSource("test_increment_only"), helpers_yt.RecipeYtTarget(string(helpers_yt.YtTestDir(t, "json_canonical"))))
	defer fixture.Teardown(t)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, fixture.Src, fixture.Dst, abstract.TransferTypeIncrementOnly)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	mysql.ExecuteMySQLStatement(t, incrementOnlySQL, fixture.SrcStorage.ConnectionParams)

	require.NoError(t, storage.WaitEqualRowsCount(t, fixture.Src.Database, "test_increment_only", fixture.SrcStorage, fixture.DstStorage, time.Second*30))
	helpers_yt.CanonizeDynamicYtTable(t, fixture.YTEnv.YT, ypath.Path(fmt.Sprintf("%s/%s_test_increment_only", fixture.YTDir, fixture.Src.Database)), "yt_table.yson")
}

// Initialization

var (
	//go:embed replication_snapshot_and_increment.sql
	snapshotAndIncrementSQL string

	//go:embed replication_increment_only.sql
	incrementOnlySQL string
)

// Helpers

func makeMysqlSource(tableName string) *provider_mysql.MysqlSource {
	srcModel := mysql.RecipeMysqlSource()
	srcModel.IncludeTableRegex = []string{tableName}
	return srcModel
}
