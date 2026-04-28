package all_types

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

func TestAllTypes(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("AllTypes", AllTypes)
	})
}

// AllTypes transfers all Oracle built-in types supported by the provider in snapshot mode,
// each group in its own table so that failures identify the exact type. Tables:
//
//   - all_types:     NUMBER, FLOAT, BINARY_FLOAT/DOUBLE, VARCHAR2, NVARCHAR2, CHAR,
//     DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, RAW, CLOB
//   - nchar_types:   NCHAR
//   - tslocal_types: TIMESTAMP WITH LOCAL TIME ZONE
//   - nclob_types:   NCLOB
//   - blob_types:    BLOB
//   - long_text:     LONG (deprecated; one per table)
//   - long_binary:   LONG RAW (deprecated; one per table)
//
// Not tested in snapshot mode:
//   - INTERVAL YEAR TO MONTH / INTERVAL DAY TO SECOND: ODPI-C (used by godror) cannot define
//     output variables for INTERVAL columns — ORA-25137 at dpiStmt_execute even after server-side
//     CAST to VARCHAR2. Intervals work in replication mode via LogMiner string format.
//   - BFILE: unsupported by provider (schema load error).
//   - ROWID/UROWID: pseudocolumn types; no practical way to insert arbitrary row-address literals.
//   - REF, user-defined types: not supported by provider.
func AllTypes(t *testing.T) {
	Source.IncludeTables = []string{
		"DT_TEST.ALL_TYPES",
		"DT_TEST.NCHAR_TYPES",
		"DT_TEST.TSLOCAL_TYPES",
		"DT_TEST.NCLOB_TYPES",
		"DT_TEST.BLOB_TYPES",
		"DT_TEST.LONG_TEXT",
		"DT_TEST.LONG_BINARY",
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, "dt_test", "all_types", 3)
	helpers.CheckRowsCount(t, &Target, "dt_test", "nchar_types", 3)
	helpers.CheckRowsCount(t, &Target, "dt_test", "tslocal_types", 3)
	helpers.CheckRowsCount(t, &Target, "dt_test", "nclob_types", 3)
	helpers.CheckRowsCount(t, &Target, "dt_test", "blob_types", 3)
	helpers.CheckRowsCount(t, &Target, "dt_test", "long_text", 3)
	helpers.CheckRowsCount(t, &Target, "dt_test", "long_binary", 3)
}
