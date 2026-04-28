package without_primary_key

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
	// Promote unique indexes to key columns when there is no explicit PRIMARY KEY.
	Source.UseUniqueIndexesAsKeys = true
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestWithoutPrimaryKey(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("WithoutPrimaryKey", WithoutPrimaryKey)
	})
}

// WithoutPrimaryKey verifies that a table with no PRIMARY KEY but with a UNIQUE INDEX
// transfers successfully when UseUniqueIndexesAsKeys is true. Oracle promotes the unique-index
// columns as the effective key, enabling upsert writes to the PostgreSQL target.
func WithoutPrimaryKey(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.NO_PK"}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, "dt_test", "no_pk", 3)

	// Verify that the unique-index column (code) is marked as key in the transferred schema.
	pgStorage := helpers.GetSampleableStorageByModel(t, &Target)
	schema, err := pgStorage.TableSchema(context.Background(), *abstract.NewTableID("dt_test", "no_pk"))
	require.NoError(t, err)
	var keyColumns []string
	for _, col := range schema.Columns() {
		if col.IsKey() {
			keyColumns = append(keyColumns, col.ColumnName)
		}
	}
	require.Equal(t, []string{"code"}, keyColumns)
}
