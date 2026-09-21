package snapshot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName  = "public"
	TransferType  = abstract.TransferTypeSnapshotOnly
	Source        = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithConnection("myConnID"))
	SrcConnection = pgrecipe.ManagedConnection(pgrecipe.WithInitDir("dump/pg"))

	Target = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                                              // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	network.InitConnectionResolver(map[string]connection.ManagedConnection{"myConnID": SrcConnection})
}

func testSnapshot(t *testing.T, source *provider_postgres.PgSource, target clickhouse_model.ChDestination) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: SrcConnection.Hosts[0].Port},
			network.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			network.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, &target, TransferType)
	delivery.Activate(t, transfer)
	require.NoError(t, storagecomparison.CompareStorages(t, source, target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
}

func TestSnapshot(t *testing.T) {
	target := Target

	testSnapshot(t, Source, target)
}
