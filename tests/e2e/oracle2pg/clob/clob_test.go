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

func TestCLOB(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Oracle source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
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

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	delivery.Activate(t, transfer)

	storagecomparison.CheckRowsCount(t, &Target, "dt_test", "docs", 3)
}

// ReadCLOBAsBLOB transfers CLOB/NCLOB columns using the ReadCLOBAsBLOB strategy, which
// reads their content as binary and maps them to PostgreSQL bytea columns.
func ReadCLOBAsBLOB(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.DOCS"}
	Source.CLOBReadingStrategy = oracle.OracleReadCLOBAsBLOB

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	delivery.Activate(t, transfer)

	storagecomparison.CheckRowsCount(t, &Target, "dt_test", "docs", 3)
}
