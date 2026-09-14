package snapshot

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/tests/e2e/mysql2ch"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = provider_mysql.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     helpers.GetIntFromEnv("RECIPE_MYSQL_PORT"),
		ServerID: 1, // what is it?
	}
	Target = clickhouse_model.ChDestination{
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "source",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)
}

func TestReplication(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------------
	// insert/update/delete several record

	connParams, err := provider_mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	client, err := provider_mysql.Connect(connParams, nil)
	require.NoError(t, err)

	_, err = client.Exec("INSERT INTO mysql_replication (id, val1, val2, b1, b8, b11) VALUES (3, 3, 'c', NULL, NULL, NULL), (4, 4, 'd', b'0', b'00000000', b'00000000000'), (5, 5, 'e', b'1', b'11111111', b'11111111111')")
	require.NoError(t, err)

	_, err = client.Exec("UPDATE mysql_replication SET val2='ee' WHERE id=5;")
	require.NoError(t, err)

	_, err = client.Query("DELETE FROM mysql_replication WHERE id=3;")
	require.NoError(t, err)

	//------------------------------------------------------------------------------------

	require.NoError(t, helpers.WaitEqualRowsCount(t, Source.Database, "mysql_replication", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator).WithPriorityComparators(mysql2ch.MySQLBytesToStringOptionalComparator)))
}
