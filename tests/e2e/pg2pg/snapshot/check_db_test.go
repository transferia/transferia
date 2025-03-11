package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Verify", Verify)
		t.Run("Snapshot", Snapshot)
	})
}

func Existence(t *testing.T) {
	_, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Verify(t *testing.T) {
	var transfer model.Transfer
	transfer.Src = &Source
	transfer.Dst = &Target
	transfer.Type = "SNAPSHOT_ONLY"

	err := tasks.VerifyDelivery(transfer, logger.Log, helpers.EmptyRegistry())
	require.NoError(t, err)

	dstStorage, err := postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)

	var result bool
	err = dstStorage.Conn.QueryRow(context.Background(), `
		SELECT EXISTS
        (
            SELECT 1
            FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename = '_ping'
        );
	`).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, false, result)
}

func Snapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
