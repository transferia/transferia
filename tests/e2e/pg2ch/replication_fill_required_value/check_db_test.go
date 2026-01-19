package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	server "github.com/transferia/transferia/pkg/abstract/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/pkg/transformer/registry/rename"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	Source.DBTables = []string{"public.customers_customerprofile"}
	Target.Cleanup = server.DisabledCleanup
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	require.NoError(t, transfer.AddExtraTransformer(rename.NewRenameTableTransformer(rename.Config{
		RenameTables: []rename.RenameTable{
			{
				OriginalName: rename.Table{
					Namespace: "public",
					Name:      "customers_customerprofile",
				},
				NewName: rename.Table{
					Namespace: "public",
					Name:      "clickhouse_chcustomerprofile",
				},
			},
		},
	})))
	tables, err := filter.NewFilter(
		[]string{"^public\\.customers_customerprofile$"}, // IncludeRegexp
		[]string{}, // ExcludeRegexp
	)
	require.NoError(t, err)
	columns, err := filter.NewFilter(
		[]string{"^id$", "^uuid$", "^bot_id$", "^full_name$", "^phone_number$"}, // IncludeRegexp
		[]string{}, // ExcludeRegexp
	)
	require.NoError(t, err)
	require.NoError(t, transfer.AddExtraTransformer(filter.NewCustomFilterColumnsTransformer(tables, columns, logger.Log)))

	err = tasks.ActivateDelivery(context.Background(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
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

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(databaseName, "clickhouse_chcustomerprofile", helpers.GetSampleableStorageByModel(t, Target), 10*time.Second, 2))
}
