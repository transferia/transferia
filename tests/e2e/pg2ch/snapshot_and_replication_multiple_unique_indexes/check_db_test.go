package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
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
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &SourcePK, &Target, TransferType)
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &SourceNoPK, &Target, TransferType)
}

func TestSnapshotAndIncrementPK(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: SourcePK.Port},
			network.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	connConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, &SourcePK)
	require.NoError(t, err)
	conn, err := provider_postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &SourcePK, &Target, TransferType)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(5 * time.Second) // for the worker to start

	_, err = conn.Exec(context.Background(), "INSERT INTO multiple_uniq_idxs_pk(a, b, c_pk, t) VALUES (3, 50, 500, 'text_5')")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "UPDATE multiple_uniq_idxs_pk SET t = 'new_text_3' WHERE b = 30")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "DELETE FROM multiple_uniq_idxs_pk WHERE a = 1")
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, "multiple_uniq_idxs_pk", storagecomparison.GetSampleableStorageByModel(t, SourcePK), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, SourcePK, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
}

func TestSnapshotAndIncrementNoPK(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: SourceNoPK.Port},
			network.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &SourceNoPK, &Target, TransferType)

	_, err := delivery.ActivateErr(transfer)
	require.Error(t, err)
}
