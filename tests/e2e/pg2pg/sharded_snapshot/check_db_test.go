package shardedsnapshot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)
}

func TestShardedSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Source.PreSteps.Constraint = true
	transfer := transferhelpers.WithLocalRuntime(
		transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly),
		2,
		1,
	)

	_, err := delivery.ActivateShardedErr(transfer, nil, nil)
	require.NoError(t, err)
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
