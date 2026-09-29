package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/serde"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_transformer "github.com/transferia/transferia/tests/helpers/transformer"
)

var (
	Source = pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.basic_types", "public.basic_types_arr"))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                                                           // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "PG source", Port: Source.Port},
	))
	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "PG source", Port: Source.Port},
		network.LabeledPort{Label: "PG target", Port: Target.Port},
	))

	//---

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debezium_parameters.DatabaseDBName:   "public",
		debezium_parameters.TopicPrefix:      "my_topic",
		debezium_parameters.AddOriginalTypes: "true",
		debezium_parameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Runtime = &abstract.LocalRuntime{
		ShardingUpload: abstract.ShardUploadParams{
			ProcessCount: 1,
		},
	}
	transfer.Src.(*provider_postgres.PgSource).NoHomo = true

	debeziumSerDeTransformer := helpers_transformer.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithCheck(emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	//---

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), `INSERT INTO public.basic_types (i) VALUES (2);`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `INSERT INTO public.basic_types_arr (i) VALUES (2);`)
	require.NoError(t, err)

	//---

	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", "basic_types", storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second, 2))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", "basic_types_arr", storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second, 2))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
	require.Equal(t, 4, serde.CountOfProcessedMessage)
}
