package multiple_schemas

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

	// The recipe user (system) has DBA privileges and can read all schemas.
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
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestMultipleSchemas(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("MultipleSchemas", MultipleSchemas)
	})
}

// MultipleSchemas verifies that a single transfer can copy tables from two distinct
// Oracle schemas (users) in one pass. The connector user is system, which has SELECT
// access to all objects in FREEPDB1.
func MultipleSchemas(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.ORDERS", "DT_TEST2.CLIENTS"}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, "dt_test", "orders", 3)
	helpers.CheckRowsCount(t, &Target, "dt_test2", "clients", 3)

	pgStorage := helpers.GetSampleableStorageByModel(t, &Target)
	for _, tableID := range []abstract.TableID{
		*abstract.NewTableID("dt_test", "orders"),
		*abstract.NewTableID("dt_test2", "clients"),
	} {
		exists, err := pgStorage.TableExists(tableID)
		require.NoError(t, err)
		require.True(t, exists, "table %s must be present in PG", tableID)
	}
}
