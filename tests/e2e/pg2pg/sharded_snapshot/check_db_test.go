package shardedsnapshot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

func TestShardedSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Source.PreSteps.Constraint = true
	transfer := helpers.WithLocalRuntime(
		helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly),
		2,
		1,
	)

	_, err := helpers.ActivateShardedErr(transfer, nil, nil)
	require.NoError(t, err)
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
