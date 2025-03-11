package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	pg_provider "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	yt_recipe "github.com/transferria/transferria/pkg/providers/yt/recipe"
	"github.com/transferria/transferria/tests/helpers"
)

func TestGroup(t *testing.T) {
	var (
		Source               = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables("public.__test"))
		Target, cleanup, err = yt_recipe.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
	)
	defer func() {
		require.NoError(t, cleanup())
	}()
	require.NoError(t, err)
	Source.WithDefaults()

	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	worker := helpers.Activate(t, transfer)

	conn, err := pg_provider.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "insert into __test (str, id, da, i) values ('qqq', 111, '1999-09-16', 1)")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "update __test set i=2 where str='qqq';")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `insert into __test (str, id, da, i) values
                                                      ('www', 111, '1999-09-16', 1),
                                                      ('eee', 111, '1999-09-16', 1),
                                                      ('rrr', 111, '1999-09-16', 1)
    `)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "delete from __test where str='rrr';")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	worker.Close(t)

	require.NoError(t, helpers.CompareStorages(t, Source, Target.LegacyModel(), helpers.NewCompareStorageParams()))
}
