package number_to_int64

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
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/oracle"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
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
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestNumberToInt64(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Oracle source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("DefaultMapping", DefaultMapping)
		t.Run("ConvertToInt64", ConvertToInt64)
	})
}

// DefaultMapping verifies that without ConvertNumberToInt64, integer NUMBER(p) columns
// arrive at PostgreSQL as numeric (arbitrary precision).
func DefaultMapping(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.NUMS"}
	Source.ConvertNumberToInt64 = false

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	delivery.Activate(t, transfer)

	storagecomparison.CheckRowsCount(t, &Target, "dt_test", "nums", 3)

	pgStorage := storagecomparison.GetSampleableStorageByModel(t, &Target)
	schema, err := pgStorage.TableSchema(context.Background(), *abstract.NewTableID("dt_test", "nums"))
	require.NoError(t, err)
	for _, col := range schema.Columns() {
		if col.ColumnName == "cnt" {
			// Without the flag, NUMBER(10) maps to a floating-point or arbitrary-precision YT type.
			require.NotEqual(t, "int64", col.DataType,
				"NUMBER(10) must NOT map to int64 without ConvertNumberToInt64")
		}
	}
}

// ConvertToInt64 verifies that with ConvertNumberToInt64=true, integer NUMBER(p) columns
// (scale=0) arrive as int64 (PostgreSQL bigint).
func ConvertToInt64(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.NUMS"}
	Source.ConvertNumberToInt64 = true

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	delivery.Activate(t, transfer)

	storagecomparison.CheckRowsCount(t, &Target, "dt_test", "nums", 3)

	pgStorage := storagecomparison.GetSampleableStorageByModel(t, &Target)
	schema, err := pgStorage.TableSchema(context.Background(), *abstract.NewTableID("dt_test", "nums"))
	require.NoError(t, err)
	for _, col := range schema.Columns() {
		if col.ColumnName == "cnt" {
			require.Equal(t, "int64", col.DataType,
				"NUMBER(10) must map to int64 with ConvertNumberToInt64=true")
		}
	}
}
