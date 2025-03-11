package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/e2e/pg2ch"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	SourcePK     = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithDBTables(`"public"."multiple_uniq_idxs_pk"`))
	SourceNoPK   = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithDBTables(`"public"."multiple_uniq_idxs_no_complete"`))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &SourcePK, &Target, TransferType)
	helpers.InitSrcDst(helpers.TransferID, &SourceNoPK, &Target, TransferType)
}

func TestSnapshotAndIncrementPK(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: SourcePK.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, &SourcePK)
	require.NoError(t, err)
	conn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	transfer := helpers.MakeTransfer(helpers.TransferID, &SourcePK, &Target, TransferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(5 * time.Second) // for the worker to start

	_, err = conn.Exec(context.Background(), "INSERT INTO multiple_uniq_idxs_pk(a, b, c_pk, t) VALUES (3, 50, 500, 'text_5')")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "UPDATE multiple_uniq_idxs_pk SET t = 'new_text_3' WHERE b = 30")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "DELETE FROM multiple_uniq_idxs_pk WHERE a = 1")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "multiple_uniq_idxs_pk", helpers.GetSampleableStorageByModel(t, SourcePK), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, SourcePK, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
}

func TestSnapshotAndIncrementNoPK(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: SourceNoPK.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, &SourceNoPK, &Target, TransferType)

	_, err := helpers.ActivateErr(transfer)
	require.Error(t, err)
}
