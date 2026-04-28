package clob

import (
	"context"
	_ "embed"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	oracle "github.com/transferia/transferia/pkg/providers/oracle"
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

func TestCLOB(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("ReadCLOB", ReadCLOBAsText)
		t.Run("ReadCLOBAsBLOB", ReadCLOBAsBLOB)
	})
}

// ReadCLOBAsText transfers CLOB/NCLOB columns using the ReadCLOB strategy, which maps
// them to PostgreSQL text columns.
func ReadCLOBAsText(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.DOCS"}
	Source.CLOBReadingStrategy = oracle.OracleReadCLOB

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, "dt_test", "docs", 3)
}

// ReadCLOBAsBLOB transfers CLOB/NCLOB columns using the ReadCLOBAsBLOB strategy, which
// reads their content as binary and maps them to PostgreSQL bytea columns.
func ReadCLOBAsBLOB(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.DOCS"}
	Source.CLOBReadingStrategy = oracle.OracleReadCLOBAsBLOB

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, "dt_test", "docs", 3)
}
