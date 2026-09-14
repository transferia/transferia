package usertypes

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"))
	Target = pgrecipe.RecipeTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                                                                          // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotOnly)
	transfer.TypeSystemVersion = 1

	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
