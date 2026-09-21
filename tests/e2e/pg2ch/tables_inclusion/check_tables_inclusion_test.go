package tables

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                                              // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func testSnapshot(t *testing.T, source *provider_postgres.PgSource, target clickhouse_model.ChDestination) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: source.Port},
			network.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			network.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()
	source.DBTables = []string{"public.__test_1", "public.__test_2", "public.__test_3"}
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, &target, TransferType)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"public.__test_1", "public.__test_2"}}

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)
}

func TestSnapshot(t *testing.T) {
	target := Target

	testSnapshot(t, Source, target)
}
