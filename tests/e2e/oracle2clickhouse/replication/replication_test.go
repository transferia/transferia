package replication

import (
	"context"
	_ "embed"
	"maps"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	oracle "github.com/transferia/transferia/pkg/providers/oracle"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/oracle"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

//go:embed dump/init.sql
var initSQL string

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement

	Source = *oraclerecipe.RecipeOracleSource()

	Target = clickhouse_model.ChDestination{
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name:  "_",
				Hosts: []string{"localhost"},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "target",
		HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             model.Drop,
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
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

func TestReplication(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Oracle source", Port: Source.Port},
			network.LabeledPort{Label: "ClickHouse target", Port: Target.NativePort},
		))
	}()

	t.Run("Group", func(t *testing.T) {
		t.Run("Replication", Replication)
	})
}

func Replication(t *testing.T) {
	Source.IncludeTables = []string{"DT_TEST.EVENTS"}

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	_ = delivery.Activate(t, transfer)

	chStorage := storagecomparison.GetSampleableStorageByModel(t, &Target)

	// Wait for initial snapshot (1 row: id=1).
	// ClickHouse destination drops the source schema and creates the table in its own database.
	require.NoError(t, storage.WaitDestinationEqualRowsCount(
		Target.Database, "events", chStorage, 30*time.Second, 1,
	))

	// DML captured by LogMiner: insert 2 rows, update id=1, delete id=2 → final: id=1 (updated), id=4
	require.NoError(t, oraclerecipe.ExecSQL(context.Background(), &Source,
		"INSERT INTO dt_test.events VALUES (2, 'second'); INSERT INTO dt_test.events VALUES (4, 'fourth'); UPDATE dt_test.events SET val = 'updated' WHERE id = 1; DELETE FROM dt_test.events WHERE id = 2; COMMIT",
	))

	require.NoError(t, storage.WaitCond(
		60*time.Second, func() bool {
			return maps.Equal(map[uint64]string{
				1: "updated",
				4: "fourth",
			}, readEvents(t, chStorage))
		},
	))
}

func readEvents(t *testing.T, destination abstract.Storage) map[uint64]string {
	result := make(map[uint64]string)
	require.NoError(t, destination.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "events",
		Schema: Target.Database,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(items []abstract.ChangeItem) error {
		for _, row := range items {
			if !row.IsRowEvent() {
				continue
			}
			var id uint64
			var val string
			var err error
			for i, name := range row.ColumnNames {
				switch strings.ToUpper(name) {
				case "ID":
					id, err = strconv.ParseUint(row.ColumnValues[i].(string), 10, 64)
					require.NoError(t, err)
				case "VAL":
					val, _ = row.ColumnValues[i].(string)
				}
			}
			result[id] = val
		}
		return nil
	}))
	return result
}
