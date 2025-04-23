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
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	tableName    = "people"
)

func TestAlters(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Target := &ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}

	t.Setenv("YC", "1")                                                  // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	time.Sleep(10 * time.Second)
	defer func() {
		sourcePort, err := helpers.GetPortFromStr(Target.Instance)
		require.NoError(t, err)
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YDB target", Port: sourcePort},
		))
	}()

	transfer := helpers.MakeTransfer(
		tableName,
		Source,
		Target,
		TransferType,
	)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	conn, err := pgcommon.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`insert into %s values(5, 'You')`, tableName))
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN new_val INTEGER`, tableName))
	require.NoError(t, err)
	t.Logf(`altering table: insert into %s values(6, 'You', 42)`, tableName)
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`insert into %s values(6, 'You', 42)`, tableName))
	require.NoError(t, err)
	t.Logf("Waiting for rows to be equal")
	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, tableName, helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(databaseName, tableName, helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 6))
}
