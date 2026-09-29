package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	yt_recipe "github.com/transferia/transferia/pkg/providers/yt/recipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
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

	targetPort, err := network.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	worker := delivery.Activate(t, transfer)

	conn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
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

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	worker.Close(t)

	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target.LegacyModel(), storagecomparison.NewCompareStorageParams()))
}
