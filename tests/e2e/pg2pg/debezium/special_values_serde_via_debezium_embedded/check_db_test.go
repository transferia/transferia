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
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/serde"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_transformer "github.com/transferia/transferia/tests/helpers/transformer"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                                                                            // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		helpers.LabeledPort{Label: "PG target", Port: Target.Port},
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

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*provider_postgres.PgSource).NoHomo = true

	debeziumSerDeTransformer := helpers_transformer.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithCheck(emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//---

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), `
		INSERT INTO public.my_table VALUES
		(
			2,

			-32768, -- t_smallint
			-2147483648, -- t_integer
			-9223372036854775808, -- t_bigint

		    -0.01,

		    '2022-08-28 19:49:47.090000Z' -- TIMESTAMPTZ
		);

		INSERT INTO public.my_table VALUES
		(
			3,

			32767, -- t_smallint
			2147483647, -- t_integer
			9223372036854775807, -- t_bigint

		    0.01,

		    '2022-08-28 19:49:47.090000Z' -- TIMESTAMPTZ
		);
    `)
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "my_table", helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 4))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
