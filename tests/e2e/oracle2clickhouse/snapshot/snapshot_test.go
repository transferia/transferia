package snapshot

import (
	"context"
	_ "embed"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/oracle"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

//go:embed dump/init.sql
var initSQL string

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	Source = *oraclerecipe.RecipeOracleSource()

	Target = clickhouse_model.ChDestination{
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name:  "_",
				Hosts: []string{"localhost"},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "target",
		HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Oracle source", Port: Source.Port},
			network.LabeledPort{Label: "ClickHouse target", Port: Target.NativePort},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
	})
}

func Snapshot(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.TEST_SNAPSHOT"}

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	delivery.Activate(t, transfer)

	storagecomparison.CheckRowsCount(t, &Target, Target.Database, "test_snapshot", 3)
}
