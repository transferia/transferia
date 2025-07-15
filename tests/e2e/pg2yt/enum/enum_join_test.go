package enum

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	pg_provider "github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var Source = pg_provider.PgSource{
	ClusterID: os.Getenv("PG_CLUSTER_ID"),
	Hosts:     []string{"localhost"},
	User:      os.Getenv("PG_LOCAL_USER"),
	Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
	Database:  os.Getenv("PG_LOCAL_DATABASE"),
	Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
	DBTables:  []string{"public.__fullnames", "public.__food_expenditure"},
}

func TestRunner(t *testing.T) {
	t.Run("TestUploadToYt", testUploadToYt)
}

// Utilities

func teardown(env *yttest.Env, p string) {
	err := env.YT.RemoveNode(
		env.Ctx,
		ypath.Path(p),
		&yt.RemoveNodeOptions{
			Recursive: true,
			Force:     true,
		},
	)
	if err != nil {
		logger.Log.Error("unable to delete test folder", log.Error(err))
	}
}

// initializes YT client and sinker config
// do not forget to call testTeardown when resources are not needed anymore
func initYt(t *testing.T, cypressPath string) (testEnv *yttest.Env, testCfg yt_provider.YtDestinationModel, testTeardown func()) {
	env, cancel := yttest.NewEnv(t)
	cfg := yt_helpers.RecipeYtTarget(cypressPath)
	return env, cfg, func() {
		teardown(env, cypressPath) // do not drop table
		cancel()
	}
}

func testUploadToYt(t *testing.T) {
	ytEnv, ytDest, cancel := initYt(t, "//home/cdc/test/TM-2118")
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	testContext, testCtxCancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer testCtxCancel()

	defer cancel()

	tableNames := []string{"__fullnames", "__food_expenditure"}
	schema := "public"

	var fullTableNames []string
	tablePaths := make([]ypath.Path, len(tableNames))
	for i, tableName := range tableNames {
		fullTableNames = append(fullTableNames, schema+"."+tableName)
		tablePaths[i] = ypath.Path(ytDest.Path()).Child(tableName)
	}
	Source.DBTables = fullTableNames

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, ytDest, abstract.TransferTypeSnapshotAndIncrement)

	// we'll compare this two quantities:

	var pgRowCount, ytRowCount int64

	// get current data from database
	srcConn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	countQuery := fmt.Sprintf(`
		SELECT count(*) FROM public.%s as E
		JOIN public.%s as N ON E.usr = N.usr;`,
		tableNames[1], tableNames[0],
	)
	rows, err := srcConn.Query(context.Background(), countQuery)
	require.NoError(t, err)
	require.True(t, rows.Next())
	err = rows.Scan(&pgRowCount)
	require.NoError(t, err)
	require.False(t, rows.Next())

	// upload tableName from public database to YT
	solomonDefaultRegistry := solomon.NewRegistry(nil)
	tables, err := tasks.ObtainAllSrcTables(transfer, solomonDefaultRegistry)
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(testContext, tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	// see how many rows in YT
	query := fmt.Sprintf(`
		SUM(1) FROM [%s] AS N
        JOIN [%s] AS E ON string(N.usr) = E.usr
        GROUP BY 1`,
		tablePaths[1], tablePaths[0])
	changesReader, err := ytEnv.YT.SelectRows(testContext, query, nil)
	require.NoError(t, err)
	require.True(t, changesReader.Next()) // can fail if empty set of rows (assume this as ytRowCount == 0)
	var any map[string]int64
	err = changesReader.Scan(&any)
	ytRowCount = any["SUM(1)"]
	require.NoError(t, err)
	require.False(t, rows.Next())

	require.Equal(t, pgRowCount, ytRowCount)
}
