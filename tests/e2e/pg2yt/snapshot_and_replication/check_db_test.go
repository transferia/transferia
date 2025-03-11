package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
	yt_helpers "github.com/transferria/transferria/tests/helpers/yt"
)

var (
	ytPath       = "//home/cdc/test/pg2yt_e2e"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))
	Target       = yt_helpers.RecipeYtTarget(ytPath)
)

func init() {
	_ = os.Setenv("YC", "1")                                              // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, TransferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------------
	// insert/update/delete several record

	exec := func(ctx context.Context, conn *pgxpool.Pool, query string) {
		rows, err := conn.Query(ctx, query)
		require.NoError(t, err)
		rows.Close()
	}

	exec(context.Background(), conn, "INSERT INTO table_simple (id, val) VALUES (2, '222'), (3, '333')")
	exec(context.Background(), conn, "UPDATE table_simple SET val='2222' WHERE id=2;")
	exec(context.Background(), conn, "DELETE FROM table_simple WHERE id=3;")

	exec(context.Background(), conn, "INSERT INTO table_simple__replica_identity_full (id, val) VALUES (2, '222'), (3, '333')")
	exec(context.Background(), conn, "UPDATE table_simple__replica_identity_full SET val='2222' WHERE id=2;")
	exec(context.Background(), conn, "DELETE FROM table_simple__replica_identity_full WHERE id=3;")

	//------------------------------------------------------------------------------------
	// wait & compare

	// table_simple__replica_identity_full won't match bcs of '__dummy' column - so we will compare only count
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "table_simple__replica_identity_full", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))

	// table_simple will match
	sourceCopy := Source
	sourceCopy.DBTables = []string{"public.table_simple"}
	require.NoError(t, helpers.CompareStorages(t, sourceCopy, Target, helpers.NewCompareStorageParams()))
}
