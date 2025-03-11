package dockercompose

import (
	"bytes"
	"context"
	_ "embed"
	"testing"

	"cuelang.org/go/pkg/time"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	dockerPgDump = []string{"docker", "run", "--network", "host", "registry.yandex.net/data-transfer/tests/base:1@sha256:48a92174b2d5917fbac6be0a48d974e3f836338acf4fa03f74fcfea7437386f1", "pg_dump"}
)

var (
	trickyTypesPg2YTSource = &postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     "postgres",
		Password: "123",
		Database: "postgres",

		DBTables:      []string{"public.pgis_supported_types"},
		Port:          7432,
		PgDumpCommand: dockerPgDump,
	}
	trickyTypesPg2YTTarget = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")

	//go:embed data/tricky_types_pg2yt/increment.sql
	trickyTypesPg2YTIncrementSQL string
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, trickyTypesPg2YTSource, trickyTypesPg2YTTarget, abstract.TransferTypeSnapshotAndIncrement)
}

type trickyTypesPg2YTCanonData struct {
	AfterSnapshot  string `json:"after_snapshot"`
	AfterIncrement string `json:"after_increment"`
}

func TestTrickyTypesPg2YTSupportedTypes(t *testing.T) {
	t.Parallel()

	ytEnv, cancelYtEnv := yttest.NewEnv(t)
	defer cancelYtEnv()

	dumpTargetDB := func() string {
		buf := bytes.NewBuffer(nil)
		require.NoError(t, yt_helpers.DumpDynamicYtTable(ytEnv.YT, ypath.Path(trickyTypesPg2YTTarget.Path()+"/pgis_supported_types"), buf))
		return buf.String()
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, trickyTypesPg2YTSource, trickyTypesPg2YTTarget, abstract.TransferTypeSnapshotAndIncrement)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	var canonData trickyTypesPg2YTCanonData
	canonData.AfterSnapshot = dumpTargetDB()

	conn, err := pgx.Connect(context.Background(), "user=postgres dbname=postgres password=123 host=localhost port=7432")
	require.NoError(t, err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), trickyTypesPg2YTIncrementSQL)
	require.NoError(t, err)

	err = helpers.WaitEqualRowsCount(t, "public", "pgis_supported_types", helpers.GetSampleableStorageByModel(t, trickyTypesPg2YTSource), helpers.GetSampleableStorageByModel(t, trickyTypesPg2YTTarget.LegacyModel()), 30*time.Second)
	require.NoError(t, err)
	canonData.AfterIncrement = dumpTargetDB()
	canon.SaveJSON(t, &canonData)
}
