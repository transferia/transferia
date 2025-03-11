package tables

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	dp_model "github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                              // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func testSnapshot(t *testing.T, source *postgres.PgSource, target model.ChDestination) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
			helpers.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			helpers.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()
	source.DBTables = []string{"public.__test_1", "public.__test_2", "public.__test_3"}
	transfer := helpers.MakeTransfer(helpers.TransferID, source, &target, TransferType)
	transfer.DataObjects = &dp_model.DataObjects{IncludeObjects: []string{"public.__test_1", "public.__test_2"}}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)
}

func TestSnapshot(t *testing.T) {
	target := Target

	testSnapshot(t, Source, target)
}
