package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/serde"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_transformer "github.com/transferia/transferia/tests/helpers/transformer"
)

var (
	Source = *mysql.RecipeMysqlSource()
	Target = *mysql.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                                                            // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: Source.Port},
			network.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	//---

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debezium_parameters.TopicPrefix:      "my_topic",
		debezium_parameters.AddOriginalTypes: "true",
		debezium_parameters.SourceType:       "mysql",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*provider_mysql.MysqlSource).PlzNoHomo = true
	transfer.Src.(*provider_mysql.MysqlSource).AllowDecimalAsFloat = true
	debeziumSerDeTransformer := helpers_transformer.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithoutCheck(emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	//---

	connParams, err := provider_mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	db, err := provider_mysql.Connect(connParams, nil)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO customers3 (pk,tinyint_,tinyint_u,smallint_,smallint_u,mediumint_,mediumint_u,int_,int_u,bigint_,bigint_u) VALUES (
			3,

			-128,
			0,

			-32768,
			0,

			-8388608,
			0,

			-2147483648,
			0,

			-9223372036854775808,
			0
		);
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO customers3 (pk,tinyint_,tinyint_u,smallint_,smallint_u,mediumint_,mediumint_u,int_,int_u,bigint_,bigint_u) VALUES (
			4,

			127,
			255,

			32767,
			65535,

			8388607,
			16777215,

			2147483647,
			4294967295,

			9223372036854775807,
			18446744073709551615
			);
	`)
	require.NoError(t, err)

	//---

	require.NoError(t, storage.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "customers3",
		storagecomparison.GetSampleableStorageByModel(t, Source),
		storagecomparison.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
