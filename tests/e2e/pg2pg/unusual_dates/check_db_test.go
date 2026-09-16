package usertypes

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"))
	Target = pgrecipe.RecipeTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                                                                          // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("UnusualDates", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
		t.Run("Replication", Replication)
	})
}

func Snapshot(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotOnly)

	_ = delivery.Activate(t, transfer)

	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}

func Replication(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	_, err = srcConn.Exec(context.Background(),
		`insert into testtable values (11, '2000-10-19 10:23:54.123', '2000-10-19 10:23:54.123+02', '2000-10-19')`)
	require.NoError(t, err)

	// BC dates will be supported in https://st.yandex-team.ru/TM-5127
	// _, err = srcConn.Exec(context.Background(),
	// 	`insert into testtable values (12, '2000-10-19 10:23:54.123 BC', '2000-10-19 10:23:54.123+02 BC', '2000-10-19 BC')`)
	// require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(),
		`insert into testtable values (13, '40000-10-19 10:23:54.123456', '40000-10-19 10:23:54.123456+02', '40000-10-19')`)
	require.NoError(t, err)

	// _, err = srcConn.Exec(context.Background(),
	// 	`insert into testtable values (14, '4000-10-19 10:23:54.123456 BC', '4000-10-19 10:23:54.123456+02 BC', '4000-10-19 BC')`)
	// require.NoError(t, err)

	require.NoError(t, storagecomparison.WaitStoragesSynced(t, Source, Target, 50, storagecomparison.NewCompareStorageParams()))
}
