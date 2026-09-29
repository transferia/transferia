package timewithfallback

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget()
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Target.Cleanup = model.DisabledCleanup
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func loadSnapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	_ = delivery.Activate(t, transfer)

	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}

// This test is kind of tricky
//
// We haven't options to turn-off CopyUpload behaviour, but we need to test behaviour on homo-inserts (who runs after COPY insert failed)
//
// So, this test initializes 'dst' table by the same table_schema, that in the 'src'.
// And except this, initialization put in 'dst' one row (which is the same as one in 'src').
// This leads to next behaviour: when COPY upload starts, COPY failed bcs of rows collision, and fallback into inserts - which successfully finished bcs of my fix.
//
// If run this test on trunk (before my fix) - it's failed.

func TestUserTypes(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	loadSnapshot(t)
}
