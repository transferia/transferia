package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/providers/ydb"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	tableName    = "people"
	primaryKey   = "ID"
)

func TestSnapshotAndIncrement(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Target := &ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}

	t.Setenv("YC", "1")                                                  // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

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
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`insert into %s values(6, 'You')`, tableName))
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`update %s set %s = 7 where %s = 6`, tableName, primaryKey, primaryKey))
	require.NoError(t, err)
	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, tableName, helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(databaseName, tableName, helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 5))
}
