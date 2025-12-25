package addcolumn

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	ydb3 "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.ytsaurus.tech/library/go/core/log"
)

func execDDL(t *testing.T, ydbConn *ydb3.Driver, query string) {
	err := ydbConn.Table().Do(context.Background(), func(ctx context.Context, session table.Session) (err error) {
		return session.ExecuteSchemeQuery(ctx, query)
	})
	require.NoError(t, err)
}

func execQuery(t *testing.T, ydbConn *ydb3.Driver, query string) {
	err := ydbConn.Table().Do(context.Background(), func(ctx context.Context, session table.Session) (err error) {
		writeTx := table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		)

		_, _, err = session.Execute(ctx, writeTx, query, nil)
		return err
	})
	require.NoError(t, err)
}

func TestAddColumnOnReplication(t *testing.T) {
	tableName := "test_table"

	source := &ydb.YdbSource{
		Token:              dp_model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{tableName},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		ChangeFeedMode:     ydb.ChangeFeedModeUpdates,
	}
	target := model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                    "default",
		Password:                "",
		Database:                "database",
		HTTPPort:                helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:              helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified:     true,
		Cleanup:                 dp_model.Drop,
		UpsertAbsentToastedRows: true,
	}
	transferType := abstract.TransferTypeIncrementOnly
	helpers.InitSrcDst(helpers.TransferID, source, &target, transferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

	ydbConn := ydbrecipe.Driver(t)

	// defer port checking
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			helpers.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()

	// create table
	execDDL(t, ydbConn, fmt.Sprintf(`
		--!syntax_v1
		CREATE TABLE %s (
			id Int64 NOT NULL,
			value Utf8,
			PRIMARY KEY (id)
		);
	`, tableName))

	// insert one rec before replication start -- will NOT be uploaded

	execQuery(t, ydbConn, fmt.Sprintf(`
		--!syntax_v1
		UPSERT INTO %s (id, value)
        VALUES  ( 1, 'Sample text'),
       			( 2, 'Sample text')
		;
	`, tableName))

	// start RETRYABLE on specific error snapshot & replication

	transfer := helpers.MakeTransfer(helpers.TransferID, source, &target, transferType)
	errCallback := func(err error) {
		if strings.Contains(err.Error(), `unable to normalize column names order for table "test_table"`) {
			logger.Log.Info("OK, correct error found in replication", log.Error(err))
		} else {
			require.NoError(t, err)
		}
	}
	worker, err := helpers.ActivateErr(transfer, errCallback)
	require.NoError(t, err)
	defer func() {
		worker.Close(t)
	}()

	// insert two more records - it's three of them now
	execQuery(t, ydbConn, fmt.Sprintf(`
		--!syntax_v1
		UPSERT INTO %s (id, value)
        VALUES  ( 3, 'Sample text'),
       			( 4, 'Sample text'),
       			( 5, 'Sample text'),
       			( 6, 'Sample text'),
       			( 7, 'Sample text'),
       			( 8, 'Sample text'),
       			( 9, 'Sample text'),
       			( 10, 'Sample text'),
       			( 11, 'Sample text')
		;
	`, tableName))

	// add new column
	execDDL(t, ydbConn, fmt.Sprintf(`
		--!syntax_v1
		ALTER TABLE %s ADD COLUMN new_column Text;
	`, tableName))

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(target.Database, tableName, helpers.GetSampleableStorageByModel(t, target), 60*time.Second, 9))

	// update old data (not required right now)
	execQuery(t, ydbConn, fmt.Sprintf(`
		--!syntax_v1
		UPDATE %s SET new_column = 'abc';
	`, tableName))

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(target.Database, tableName, helpers.GetSampleableStorageByModel(t, target), 60*time.Second, 11))

	// insert more records - it's 18 of them now, +2 previous after update = 20
	execQuery(t, ydbConn, fmt.Sprintf(`
		--!syntax_v1
		UPSERT INTO %s (id, value, new_column)
        VALUES  ( 12, 'Sample text', 'n'),
       			( 13, 'Sample text', 'n'),
       			( 14, 'Sample text', 'n'),
       			( 15, 'Sample text', 'n'),
       			( 16, 'Sample text', 'n'),
       			( 17, 'Sample text', 'n'),
       			( 18, 'Sample text', 'n'),
       			( 19, 'Sample text', 'n'),
       			( 20, 'Sample text', 'n')
		;
	`, tableName))

	// wait a little bit until 18 data lines
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(target.Database, tableName, helpers.GetSampleableStorageByModel(t, target), 60*time.Second, 20))

	// update 2nd rec
	// update even more data
	execQuery(t, ydbConn, fmt.Sprintf(`
		--!syntax_v1
		UPDATE %s SET new_column = 'abc' WHERE id >= 6 AND id < 16;
	`, tableName))

	// delete some record
	execQuery(t, ydbConn, fmt.Sprintf(`
		--!syntax_v1
		DELETE FROM %s WHERE id = 17;
	`, tableName))

	// check
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(target.Database, tableName, helpers.GetSampleableStorageByModel(t, target), 60*time.Second, 19))
}
