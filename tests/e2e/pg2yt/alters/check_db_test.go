package alters

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
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	yt_main "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	Source = postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test_a", "public.__test_b", "public.__test_c", "public.__test_d"},
		SlotID:    "test_slot_id",
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e_alters")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_alters"), yt_main.NodeMap, &yt_main.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_alters"), &yt_main.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Load", Load)
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	srcConnConfig, err := postgres.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	srcConnConfig.PreferSimpleProtocol = true
	srcConn, err := postgres.NewPgConnPool(srcConnConfig, nil)
	require.NoError(t, err)

	//------------------------------------------------------------------------------

	err = postgres.CreateReplicationSlot(&Source)
	require.NoError(t, err)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	//------------------------------------------------------------------------------

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------

	insertBeforeA := "INSERT INTO public.__test_a (a_id, a_name) VALUES (3, 'Bee for ALTER');"
	_, err = srcConn.Exec(context.Background(), insertBeforeA)
	require.NoError(t, err)

	insertBeforeB := "INSERT INTO public.__test_b (b_id, b_name, b_address) VALUES (3, 'Rachel', 'Baker Street, 2');"
	_, err = srcConn.Exec(context.Background(), insertBeforeB)
	require.NoError(t, err)

	insertBeforeC := "INSERT INTO public.__test_c (c_id, c_uid, c_name) VALUES (3, 48, 'Dell GTX-5667');"
	_, err = srcConn.Exec(context.Background(), insertBeforeC)
	require.NoError(t, err)

	insertBeforeD := "INSERT INTO public.__test_d (d_id, d_uid, d_name) VALUES (3, 34, 'Distributed Systems');"
	_, err = srcConn.Exec(context.Background(), insertBeforeD)
	require.NoError(t, err)

	var checkSourceRowCount int
	rowsNumberA := "SELECT SUM(1) FROM public.__test_a"
	err = srcConn.QueryRow(context.Background(), rowsNumberA).Scan(&checkSourceRowCount)
	require.NoError(t, err)
	require.Equal(t, 3, checkSourceRowCount)

	rowsNumberB := "SELECT SUM(1) FROM public.__test_b"
	err = srcConn.QueryRow(context.Background(), rowsNumberB).Scan(&checkSourceRowCount)
	require.NoError(t, err)
	require.Equal(t, 3, checkSourceRowCount)

	rowsNumberC := "SELECT SUM(1) FROM public.__test_c"
	err = srcConn.QueryRow(context.Background(), rowsNumberC).Scan(&checkSourceRowCount)
	require.NoError(t, err)
	require.Equal(t, 3, checkSourceRowCount)

	rowsNumberD := "SELECT SUM(1) FROM public.__test_d"
	err = srcConn.QueryRow(context.Background(), rowsNumberD).Scan(&checkSourceRowCount)
	require.NoError(t, err)
	require.Equal(t, 3, checkSourceRowCount)

	//------------------------------------------------------------------------------

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test_a", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test_b", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test_c", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test_d", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	logger.Log.Info("wait 30 seconds for slot to move LSN")
	time.Sleep(30 * time.Second)

	//------------------------------------------------------------------------------

	alterRequestA := "ALTER TABLE public.__test_a ADD a_current_time TIMESTAMP;"
	_, err = srcConn.Exec(context.Background(), alterRequestA)
	require.NoError(t, err)

	alterRequestB := "ALTER TABLE public.__test_b DROP COLUMN b_address;"
	_, err = srcConn.Exec(context.Background(), alterRequestB)
	require.NoError(t, err)

	alterRequestC := "ALTER TABLE public.__test_c DROP COLUMN c_uid;"
	_, err = srcConn.Exec(context.Background(), alterRequestC)
	require.NoError(t, err)

	alterRequestExtensionD := "ALTER TABLE public.__test_d ALTER COLUMN d_id SET DATA TYPE bigint;"
	_, err = srcConn.Exec(context.Background(), alterRequestExtensionD)
	require.NoError(t, err)

	alterRequestNarrowingD := "ALTER TABLE public.__test_d ALTER COLUMN d_uid SET DATA TYPE int;"
	_, err = srcConn.Exec(context.Background(), alterRequestNarrowingD)
	require.NoError(t, err)

	var checkTypeD string
	requestCheckTypeD := "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '__test_d' AND COLUMN_NAME = 'd_uid'"
	err = srcConn.QueryRow(context.Background(), requestCheckTypeD).Scan(&checkTypeD)
	require.NoError(t, err)
	require.Equal(t, "integer", checkTypeD)

	// ---------------------------------------------------------------------

	insertAfterA := "INSERT INTO public.__test_a (a_id, a_name, a_current_time) VALUES (4, 'Happy Tester', now());"
	_, err = srcConn.Exec(context.Background(), insertAfterA)
	require.NoError(t, err)

	insertAfterB := "INSERT INTO public.__test_b (b_id, b_name) VALUES (4, 'Katrin');"
	_, err = srcConn.Exec(context.Background(), insertAfterB)
	require.NoError(t, err)

	insertAfterC := "INSERT INTO public.__test_c (c_id, c_name) VALUES (4, 'Lenovo ThinkPad Pro');"
	_, err = srcConn.Exec(context.Background(), insertAfterC)
	require.NoError(t, err)

	requestCorrectD := "INSERT INTO public.__test_d (d_id, d_uid, d_name) VALUES (2147483648, 0, 'Joseph');"
	_, err = srcConn.Exec(context.Background(), requestCorrectD)
	require.NoError(t, err)

	requestIncorrectD := "INSERT INTO public.__test_d (d_id, d_uid, d_name) VALUES (1337, 2147483648, 'Alex');"
	_, err = srcConn.Exec(context.Background(), requestIncorrectD)
	require.Error(t, err)

	srcConn.Close()

	// ---------------------------------------------------------------------

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test_a", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test_b", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test_c", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test_d", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
}
