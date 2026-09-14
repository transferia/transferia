package snapshot

import (
	"os"
	"testing"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"), pgrecipe.WithDBTables("public.__test"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshot(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, Target, "public", "__test", 1)
}
