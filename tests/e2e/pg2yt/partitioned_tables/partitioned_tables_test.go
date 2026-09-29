package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement

	SourceWithCollapse   = newSource(true, nil)
	TargetWithCollapse   = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e/with_collapse")
	TransferWithCollapse = transferhelpers.MakeTransfer("test_slot_id_with_collapse", &SourceWithCollapse, TargetWithCollapse, TransferType)

	SourceWithCollapseOnlyParts = newSource(true, []string{
		"public.measurement_inherited_y2006m02",
		"public.measurement_inherited_y2006m03",
		"public.measurement_inherited_y2006m04",
		"public.measurement_declarative_y2006m02",
		"public.measurement_declarative_y2006m03",
		"public.measurement_declarative_y2006m04",
		"public.measurement_declarative_y2006m05",
	})
	TargetWithCollapseOnlyParts   = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e/with_collapse_only_parts")
	TransferWithCollapseOnlyParts = transferhelpers.MakeTransfer("test_slot_id_with_collapse_only_parts", &SourceWithCollapseOnlyParts, TargetWithCollapseOnlyParts, TransferType)

	SourceWithoutCollapse   = newSource(false, nil)
	TargetWithoutCollapse   = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e/without_collapse")
	TransferWithoutCollapse = transferhelpers.MakeTransfer("test_slot_id_without_collapse", &SourceWithoutCollapse, TargetWithoutCollapse, TransferType)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestGroup(t *testing.T) {
	targetPort, err := network.GetPortFromStr(TargetWithCollapse.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: SourceWithCollapse.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	SourceWithCollapse.WithDefaults()
	SourceWithCollapseOnlyParts.WithDefaults()
	SourceWithoutCollapse.WithDefaults()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Load", Load)
	})
}

func Load(t *testing.T) {
	workerWithCollapse := delivery.Activate(t, TransferWithCollapse)
	defer workerWithCollapse.Close(t)

	workerWithCollapseOnlyParts := delivery.Activate(t, TransferWithCollapseOnlyParts)
	defer workerWithCollapseOnlyParts.Close(t)

	workerWithoutCollapse := delivery.Activate(t, TransferWithoutCollapse)
	defer workerWithoutCollapse.Close(t)

	srcStorage, err := provider_postgres.NewStorage(SourceWithCollapse.ToStorageParams(nil))
	require.NoError(t, err)

	//-----------------------------------------------------------------------------------------------------------------
	// update tables in source

	updateInheritedTable(t, srcStorage)
	updateDeclarativeTable(t, srcStorage)

	//-----------------------------------------------------------------------------------------------------------------

	checkRowsCountInSource(t)
	checkRowsCountInTargetWithoutCollapse(t)
	checkRowsCountInTargetWithCollapse(t)
	checkRowsCountInTargetWithCollapseOnlyParts(t)
}

func checkRowsCountInSource(t *testing.T) {
	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_inherited", 10)
	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_inherited_y2006m02", 3)
	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_inherited_y2006m03", 4)
	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_inherited_y2006m04", 3)

	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative", 12)
	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative_y2006m02", 3)
	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative_y2006m03", 4)
	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative_y2006m04", 3)
	storagecomparison.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative_y2006m05", 2)
}

func checkRowsCountInTargetWithCollapse(t *testing.T) {
	sourceStorage := storagecomparison.GetSampleableStorageByModel(t, SourceWithCollapse)
	targetStorage := storagecomparison.GetSampleableStorageByModel(t, TargetWithCollapse)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_declarative", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_inherited", sourceStorage, targetStorage, 60*time.Second))
}

func checkRowsCountInTargetWithCollapseOnlyParts(t *testing.T) {
	sourceStorage := storagecomparison.GetSampleableStorageByModel(t, SourceWithCollapseOnlyParts)
	targetStorage := storagecomparison.GetSampleableStorageByModel(t, TargetWithCollapseOnlyParts)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_declarative", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_inherited", sourceStorage, targetStorage, 60*time.Second))
}

func checkRowsCountInTargetWithoutCollapse(t *testing.T) {
	sourceStorage := storagecomparison.GetSampleableStorageByModel(t, SourceWithoutCollapse)
	targetStorage := storagecomparison.GetSampleableStorageByModel(t, TargetWithoutCollapse)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m03", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m04", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m03", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m04", sourceStorage, targetStorage, 60*time.Second))
}

func updateInheritedTable(t *testing.T, srcStorage *provider_postgres.Storage) {
	_, err := srcStorage.Conn.Exec(context.Background(), `
        insert into measurement_inherited values
        (6, '2006-02-02', 1),
        (7, '2006-02-02', 1),
        (8, '2006-03-02', 1);
        `)
	require.NoError(t, err)

	_, err = srcStorage.Conn.Exec(context.Background(), `
        update measurement_inherited
        set logdate = '2006-02-10'
        where id = 6;
        `)
	require.NoError(t, err)

	_, err = srcStorage.Conn.Exec(context.Background(), `
        update measurement_inherited
        set logdate = '2006-02-20', id = 8
        where id = 7;
        `)
	require.NoError(t, err)

	_, err = srcStorage.Conn.Exec(context.Background(), `
        delete from measurement_inherited
        where id = 1;
        `)
	require.NoError(t, err)
}

func updateDeclarativeTable(t *testing.T, srcStorage *provider_postgres.Storage) {
	_, err := srcStorage.Conn.Exec(context.Background(), `
        insert into measurement_declarative values
        (6, '2006-02-02', 1),
        (7, '2006-02-02', 1),
        (8, '2006-03-02', 1);
        `)
	require.NoError(t, err)

	_, err = srcStorage.Conn.Exec(context.Background(), `
        update measurement_declarative
        set logdate = '2006-02-10'
        where id = 6;
        `)
	require.NoError(t, err)

	_, err = srcStorage.Conn.Exec(context.Background(), `
        update measurement_declarative
        set logdate = '2006-02-20', id = 8
        where id = 7;
        `)
	require.NoError(t, err)

	_, err = srcStorage.Conn.Exec(context.Background(), `
        delete from measurement_declarative
        where id = 1;
        `)
	require.NoError(t, err)
}

func newSource(collapseInheritTables bool, tables []string) provider_postgres.PgSource {
	return provider_postgres.PgSource{
		Hosts:                 []string{"localhost"},
		ClusterID:             os.Getenv("SOURCE_CLUSTER_ID"),
		User:                  os.Getenv("PG_LOCAL_USER"),
		Password:              model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:              os.Getenv("PG_LOCAL_DATABASE"),
		Port:                  testenv.GetIntFromEnv("PG_LOCAL_PORT"),
		UseFakePrimaryKey:     true, // we use PG receipe with outdated 10.5 version that doesn`t allow set primary or unique keys on virtual parent(declarative) tables
		CollapseInheritTables: collapseInheritTables,
		DBTables:              tables,
	}
}
