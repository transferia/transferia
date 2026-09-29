package snapshot

import (
	"context"
	_ "embed"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
	_ "github.com/transferia/transferia/tests/helpers/registration/oracle"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

//go:embed dump/init.sql
var initSQL string

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	Source = *oraclerecipe.RecipeOracleSource()
	Target = *mysqlrecipe.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")
	Source.ConvertNumberToInt64 = true // Mysql does not allow key of type string without length limit
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Oracle source", Port: Source.Port},
			network.LabeledPort{Label: "MySQL target", Port: Target.Port},
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

	storagecomparison.CheckRowsCount(t, Target, Target.Database, "test_snapshot", 3)
}
