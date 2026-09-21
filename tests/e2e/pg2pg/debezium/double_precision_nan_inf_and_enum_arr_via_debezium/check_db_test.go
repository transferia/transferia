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
	"github.com/transferia/transferia/tests/helpers/postgres"
	"github.com/transferia/transferia/tests/helpers/serde"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testsflag"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_transformer "github.com/transferia/transferia/tests/helpers/transformer"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget()
)

var insertStmt = `
INSERT INTO public.user_table VALUES (5,123,ARRAY ['VALUE_THREE','VALUE_ONE']::my_enum_type[]);
INSERT INTO public.user_table VALUES (6,321,null);

-- TODO: for this cases we need to update wal2json
-- INSERT INTO public.double_precision_values VALUES (4,'-Infinity');
-- INSERT INTO public.double_precision_values VALUES (5,'Infinity');
-- INSERT INTO public.double_precision_values VALUES (6,'NaN');
`

func init() {
	_ = os.Setenv("YC", "1")                                                                                            // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
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

	testsflag.TurnOff()
	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debezium_parameters.DatabaseDBName:   "public",
		debezium_parameters.TopicPrefix:      "my_topic",
		debezium_parameters.AddOriginalTypes: "true",
		debezium_parameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*provider_postgres.PgSource).NoHomo = true

	debeziumSerDeTransformer := helpers_transformer.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithCheck(emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	//---

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), insertStmt)
	require.NoError(t, err)

	//---

	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", "user_table", storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second, 6))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithPriorityComparators(postgres.PgDebeziumIgnoreTemporalAccuracyForArraysComparator)))
	require.Equal(t, 6, serde.CountOfProcessedMessage)
}
