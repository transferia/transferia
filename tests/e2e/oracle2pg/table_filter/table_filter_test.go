package table_filter

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
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestTableFilter(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("IncludeTables", FilterByInclude)
		t.Run("ExcludeTables", FilterByExclude)
	})
}

func FilterByInclude(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.TBL_A", "DT_TEST.TBL_B"}
	Source.ExcludeTables = nil

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, "dt_test", "tbl_a", 2)
	helpers.CheckRowsCount(t, &Target, "dt_test", "tbl_b", 2)

	pgStorage := helpers.GetSampleableStorageByModel(t, &Target)
	exists, err := pgStorage.TableExists(*abstract.NewTableID("dt_test", "tbl_c"))
	require.NoError(t, err)
	require.False(t, exists, "tbl_c must be absent when IncludeTables excludes it")
}

func FilterByExclude(t *testing.T) {
	Source.IncludeTables = nil
	Source.ExcludeTables = []string{"DT_TEST.TBL_C"}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, "dt_test", "tbl_a", 2)
	helpers.CheckRowsCount(t, &Target, "dt_test", "tbl_b", 2)

	pgStorage := helpers.GetSampleableStorageByModel(t, &Target)
	exists, err := pgStorage.TableExists(*abstract.NewTableID("dt_test", "tbl_c"))
	require.NoError(t, err)
	require.False(t, exists, "tbl_c must be absent when ExcludeTables contains it")
}
