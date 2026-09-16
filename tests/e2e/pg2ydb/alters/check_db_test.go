package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	tableName    = "people"
)

func TestAlters(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Target := &provider_ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}

	t.Setenv("YC", "1")                                                                  // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	time.Sleep(10 * time.Second)
	defer func() {
		sourcePort, err := network.GetPortFromStr(Target.Instance)
		require.NoError(t, err)
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "YDB target", Port: sourcePort},
		))
	}()

	transfer := transferhelpers.MakeTransfer(
		tableName,
		Source,
		Target,
		TransferType,
	)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	conn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`insert into %s values(5, 'You')`, tableName))
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN new_val INTEGER`, tableName))
	require.NoError(t, err)
	t.Logf(`altering table: insert into %s values(6, 'You', 42)`, tableName)
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`insert into %s values(6, 'You', 42)`, tableName))
	require.NoError(t, err)
	t.Logf("Waiting for rows to be equal")
	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, tableName, storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storage.WaitDestinationEqualRowsCount(databaseName, tableName, storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second, 6))
}
