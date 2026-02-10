package snapshot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	databaseName  = "public"
	TransferType  = abstract.TransferTypeSnapshotOnly
	Source        = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithConnection("myConnID"))
	SrcConnection = pgrecipe.ManagedConnection(pgrecipe.WithInitDir("dump/pg"))

	Target = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                              // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	helpers.InitConnectionResolver(map[string]connection.ManagedConnection{"myConnID": SrcConnection})
}

func testSnapshot(t *testing.T, source *postgres.PgSource, target model.ChDestination) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: SrcConnection.Hosts[0].Port},
			helpers.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			helpers.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, &target, TransferType)
	helpers.Activate(t, transfer)
	require.NoError(t, helpers.CompareStorages(t, source, target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
}

func TestSnapshot(t *testing.T) {
	target := Target

	testSnapshot(t, Source, target)
}
