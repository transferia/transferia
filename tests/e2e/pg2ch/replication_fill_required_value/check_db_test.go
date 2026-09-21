package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	transformer_filter "github.com/transferia/transferia/pkg/transformer/registry/filter"
	transformer_rename "github.com/transferia/transferia/pkg/transformer/registry/rename"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	connConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := provider_postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	Source.DBTables = []string{"public.customers_customerprofile"}
	Target.Cleanup = model.DisabledCleanup
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	require.NoError(t, transfer.AddExtraTransformer(transformer_rename.NewRenameTableTransformer(transformer_rename.Config{
		RenameTables: []transformer_rename.RenameTable{
			{
				OriginalName: transformer_rename.Table{
					Namespace: "public",
					Name:      "customers_customerprofile",
				},
				NewName: transformer_rename.Table{
					Namespace: "public",
					Name:      "clickhouse_chcustomerprofile",
				},
			},
		},
	})))
	tables, err := transformer_filter.NewFilter(
		[]string{"^public\\.customers_customerprofile$"}, // IncludeRegexp
		[]string{}, // ExcludeRegexp
	)
	require.NoError(t, err)
	columns, err := transformer_filter.NewFilter(
		[]string{"^id$", "^uuid$", "^bot_id$", "^full_name$", "^phone_number$"}, // IncludeRegexp
		[]string{}, // ExcludeRegexp
	)
	require.NoError(t, err)
	require.NoError(t, transfer.AddExtraTransformer(transformer_filter.NewCustomFilterColumnsTransformer(tables, columns, logger.Log)))

	err = tasks.ActivateDelivery(context.Background(), nil, coordinator.NewFakeClient(), *transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// insert/update/delete several record

	queries := []string{
		`
		insert into customers_customerprofile (id, created_at, last_active_at, variable_dict, bot_id, profile_id, uuid, messenger_id, platform, viber_api_version, chat_center_mode, god_mode, status, status_changed)
		values (1, '2004-10-19 10:23:54+02', '2004-10-19 10:23:54+02', '{}', 0, 0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'messenger_id', 'platform', 0, true, true, 'status', '2004-10-19 10:23:54+02')
		;
		insert into customers_customerprofile (id, created_at, last_active_at, variable_dict, bot_id, profile_id, uuid, messenger_id, platform, viber_api_version, chat_center_mode, god_mode, status, status_changed)
		values (2, '2004-10-19 10:23:54+02', '2004-10-19 10:23:54+02', '{}', 0, 0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'messenger_id', 'platform', 0, true, true, 'status', '2004-10-19 10:23:54+02')
		;`,
		`
		delete from customers_customerprofile where id=0;`,
	}

	for _, query := range queries {
		rows, err := conn.Query(context.Background(), query)
		require.NoError(t, err)
		rows.Close()
	}

	time.Sleep(time.Second)

	//------------------------------------------------------------------------------------
	// wait & compare

	require.NoError(t, storage.WaitDestinationEqualRowsCount(databaseName, "clickhouse_chcustomerprofile", storagecomparison.GetSampleableStorageByModel(t, Target), 10*time.Second, 2))
}
