package shardedsnapshot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/tests/e2e/mysql2ch"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = provider_mysql.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     helpers.GetIntFromEnv("RECIPE_MYSQL_PORT"),
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
	_ = os.Setenv("YC", "1")
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

func TestShardedSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MySQL source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	transfer := helpers.WithLocalRuntime(
		helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly),
		2,
		1,
	)
	_, err := helpers.ActivateShardedErr(transfer, nil, nil)
	require.NoError(t, err)
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator).WithPriorityComparators(mysql2ch.MySQLBytesToStringOptionalComparator)))
}
