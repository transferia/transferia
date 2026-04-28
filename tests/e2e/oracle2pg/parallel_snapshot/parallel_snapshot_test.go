package parallel_snapshot

import (
	"context"
	_ "embed"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
)

//go:embed dump/init.sql
var initSQL string

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	Source = *oraclerecipe.RecipeOracleSource()

	dstPort, _ = strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	Target     = provider_postgres.PgDestination{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database: os.Getenv("PG_LOCAL_DATABASE"),
		Port:     dstPort,
		Cleanup:  model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")
	// Split the table across 4 ROWID ranges, each loaded by a separate reader goroutine.
	Source.UseParallelTableLoad = true
	Source.ParallelTableLoadDegreeOfParallelism = 4
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestParallelSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("ParallelSnapshot", ParallelSnapshot)
	})
}

// ParallelSnapshot verifies that UseParallelTableLoad produces the same number of rows
// as a sequential load — ROWID-range splits must neither duplicate nor drop rows.
func ParallelSnapshot(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.BIG_TABLE"}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, "dt_test", "big_table", 2000)
}
