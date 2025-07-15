package decimal

import (
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
)

// Test cases

func TestSnapshotAndReplication(t *testing.T) {
	fixture := helpers.SetupMySQL2YTTest(t, makeMysqlSource("test_snapshot_and_increment"), yt_helpers.RecipeYtTarget(string(yt_helpers.YtTestDir(t, "decimal"))))
	defer fixture.Teardown(t)

	transfer := helpers.MakeTransfer(helpers.TransferID, fixture.Src, fixture.Dst, abstract.TransferTypeSnapshotAndIncrement)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	helpers.ExecuteMySQLStatement(t, snapshotAndIncrementSQL, fixture.SrcStorage.ConnectionParams)

	require.NoError(t, helpers.WaitEqualRowsCount(t, fixture.Src.Database, "test_snapshot_and_increment", fixture.SrcStorage, fixture.DstStorage, time.Second*30))
	yt_helpers.CanonizeDynamicYtTable(t, fixture.YTEnv.YT, ypath.Path(fmt.Sprintf("%s/%s_test_snapshot_and_increment", fixture.YTDir, fixture.Src.Database)), "yt_table.yson")
}

func TestReplication(t *testing.T) {
	fixture := helpers.SetupMySQL2YTTest(t, makeMysqlSource("test_increment_only"), yt_helpers.RecipeYtTarget(string(yt_helpers.YtTestDir(t, "decimal"))))
	defer fixture.Teardown(t)

	transfer := helpers.MakeTransfer(helpers.TransferID, fixture.Src, fixture.Dst, abstract.TransferTypeIncrementOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	helpers.ExecuteMySQLStatement(t, incrementOnlySQL, fixture.SrcStorage.ConnectionParams)

	require.NoError(t, helpers.WaitEqualRowsCount(t, fixture.Src.Database, "test_increment_only", fixture.SrcStorage, fixture.DstStorage, time.Second*30))
	yt_helpers.CanonizeDynamicYtTable(t, fixture.YTEnv.YT, ypath.Path(fmt.Sprintf("%s/%s_test_increment_only", fixture.YTDir, fixture.Src.Database)), "yt_table.yson")
}

// Initialization

var (
	//go:embed replication_snapshot_and_increment.sql
	snapshotAndIncrementSQL string

	//go:embed replication_increment_only.sql
	incrementOnlySQL string
)

// Helpers

func makeMysqlSource(tableName string) *mysql.MysqlSource {
	srcModel := helpers.RecipeMysqlSource()
	srcModel.IncludeTableRegex = []string{tableName}
	srcModel.AllowDecimalAsFloat = true
	return srcModel
}
