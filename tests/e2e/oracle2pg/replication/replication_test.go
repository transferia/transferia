package replication

import (
	"context"
	_ "embed"
	"os"
	"strconv"
	"testing"
	"time"

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
	TransferType = abstract.TransferTypeSnapshotAndIncrement

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
	// InMemoryLogTracker tracks the SCN position in process memory; sufficient for test use.
	Source.TrackerType = oracle.OracleInMemoryLogTracker
	Source.IsNonConsistentSnapshot = false
	// DBMS_LOGMNR must run from CDB$ROOT (ORA-65040 otherwise). Setting PDB causes
	// CDBQueryGlobal to issue "ALTER SESSION SET CONTAINER = cdb$root" before LogMiner
	// calls, while PDBQueryGlobal still switches to FREEPDB1 for data queries.
	Source.PDB = os.Getenv("RECIPE_ORACLE_SERVICE")
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestReplication(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("Replication", Replication)
	})
}

func Replication(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.EVENTS"}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	_ = helpers.Activate(t, transfer)

	pgStorage := helpers.GetSampleableStorageByModel(t, &Target)

	// Wait for initial snapshot (1 row: id=1)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		"dt_test", "events", pgStorage, 30*time.Second, 1,
	))

	// DML captured by LogMiner: insert 2 rows, update id=1, delete id=2 → final: id=1 (updated), id=4
	require.NoError(t, oraclerecipe.ExecSQL(context.Background(), &Source,
		"INSERT INTO dt_test.events VALUES (2, 'second'); INSERT INTO dt_test.events VALUES (4, 'fourth'); UPDATE dt_test.events SET val = 'updated' WHERE id = 1; DELETE FROM dt_test.events WHERE id = 2; COMMIT",
	))

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		"dt_test", "events", pgStorage, 60*time.Second, 2,
	))
}
