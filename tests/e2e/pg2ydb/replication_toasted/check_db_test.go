package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/ydb"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	tableName    = "test"
)

func TestSnapshotAndIncrement(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Target := &provider_ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}

	t.Setenv("YC", "1")                                                                  // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

	defer func() {
		sourcePort, err := helpers.GetPortFromStr(Target.Instance)
		require.NoError(t, err)
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YDB target", Port: sourcePort},
		))
	}()

	connConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, Source)
	require.NoError(t, err)
	conn, err := provider_postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, TransferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(5 * time.Second) // for the worker to start

	_, err = conn.Exec(context.Background(), "INSERT INTO test (i1, t1, i2, t2, vc1) VALUES (3, '3', 3, 'c', '3'), (4, '4', 4, 'd', '4')")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "UPDATE test SET t2 = 'test_update' WHERE i1 = 1")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "DELETE FROM test WHERE i1 != 1")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, tableName, helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(databaseName, tableName, helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 1))

	var large string
	var small string
	err = backoff.Retry(func() error {
		return conn.QueryRow(context.Background(), "SELECT t2, vc1 FROM public.test WHERE i1 = 1").Scan(&small, &large)
	}, backoff.NewConstantBackOff(time.Second))
	require.NoError(t, err)

	dump := ydb.PullDataFromTable(t,
		os.Getenv("YDB_TOKEN"),
		helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		"public_test")
	require.Equal(t, 1, len(dump))

	idx := dump[0].ColumnNameIndex("vc1")
	require.True(t, idx != -1)
	require.Equal(t, large, dump[0].ColumnValues[idx])

	idx = dump[0].ColumnNameIndex("t2")
	require.True(t, idx != -1)
	require.Equal(t, small, dump[0].ColumnValues[idx])
}
