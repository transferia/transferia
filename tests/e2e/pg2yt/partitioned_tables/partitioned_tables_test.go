package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement

	SourceWithCollapse   = newSource(true, nil)
	TargetWithCollapse   = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e/with_collapse")
	TransferWithCollapse = helpers.MakeTransfer("test_slot_id_with_collapse", &SourceWithCollapse, TargetWithCollapse, TransferType)

	SourceWithCollapseOnlyParts = newSource(true, []string{
		"public.measurement_inherited_y2006m02",
		"public.measurement_inherited_y2006m03",
		"public.measurement_inherited_y2006m04",
		"public.measurement_declarative_y2006m02",
		"public.measurement_declarative_y2006m03",
		"public.measurement_declarative_y2006m04",
		"public.measurement_declarative_y2006m05",
	})
	TargetWithCollapseOnlyParts   = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e/with_collapse_only_parts")
	TransferWithCollapseOnlyParts = helpers.MakeTransfer("test_slot_id_with_collapse_only_parts", &SourceWithCollapseOnlyParts, TargetWithCollapseOnlyParts, TransferType)

	SourceWithoutCollapse   = newSource(false, nil)
	TargetWithoutCollapse   = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e/without_collapse")
	TransferWithoutCollapse = helpers.MakeTransfer("test_slot_id_without_collapse", &SourceWithoutCollapse, TargetWithoutCollapse, TransferType)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(TargetWithCollapse.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: SourceWithCollapse.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
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
	workerWithCollapse := helpers.Activate(t, TransferWithCollapse)
	defer workerWithCollapse.Close(t)

	workerWithCollapseOnlyParts := helpers.Activate(t, TransferWithCollapseOnlyParts)
	defer workerWithCollapseOnlyParts.Close(t)

	workerWithoutCollapse := helpers.Activate(t, TransferWithoutCollapse)
	defer workerWithoutCollapse.Close(t)

	srcStorage, err := postgres.NewStorage(SourceWithCollapse.ToStorageParams(nil))
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
	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_inherited", 10)
	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_inherited_y2006m02", 3)
	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_inherited_y2006m03", 4)
	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_inherited_y2006m04", 3)

	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative", 12)
	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative_y2006m02", 3)
	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative_y2006m03", 4)
	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative_y2006m04", 3)
	helpers.CheckRowsCount(t, SourceWithCollapse, "public", "measurement_declarative_y2006m05", 2)
}

func checkRowsCountInTargetWithCollapse(t *testing.T) {
	sourceStorage := helpers.GetSampleableStorageByModel(t, SourceWithCollapse)
	targetStorage := helpers.GetSampleableStorageByModel(t, TargetWithCollapse)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited", sourceStorage, targetStorage, 60*time.Second))
}

func checkRowsCountInTargetWithCollapseOnlyParts(t *testing.T) {
	sourceStorage := helpers.GetSampleableStorageByModel(t, SourceWithCollapseOnlyParts)
	targetStorage := helpers.GetSampleableStorageByModel(t, TargetWithCollapseOnlyParts)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited", sourceStorage, targetStorage, 60*time.Second))
}

func checkRowsCountInTargetWithoutCollapse(t *testing.T) {
	sourceStorage := helpers.GetSampleableStorageByModel(t, SourceWithoutCollapse)
	targetStorage := helpers.GetSampleableStorageByModel(t, TargetWithoutCollapse)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m03", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m04", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m03", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m04", sourceStorage, targetStorage, 60*time.Second))
}

func updateInheritedTable(t *testing.T, srcStorage *postgres.Storage) {
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

func updateDeclarativeTable(t *testing.T, srcStorage *postgres.Storage) {
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

func newSource(collapseInheritTables bool, tables []string) postgres.PgSource {
	return postgres.PgSource{
		Hosts:                 []string{"localhost"},
		ClusterID:             os.Getenv("SOURCE_CLUSTER_ID"),
		User:                  os.Getenv("PG_LOCAL_USER"),
		Password:              model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:              os.Getenv("PG_LOCAL_DATABASE"),
		Port:                  helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		UseFakePrimaryKey:     true, // we use PG receipe with outdated 10.5 version that doesn`t allow set primary or unique keys on virtual parent(declarative) tables
		CollapseInheritTables: collapseInheritTables,
		DBTables:              tables,
	}
}
