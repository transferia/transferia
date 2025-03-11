package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement

	TruncateSource = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"), pgrecipe.WithEdit(func(pg *postgres.PgSource) {
		pg.UseFakePrimaryKey = true
	}))
	TruncateTarget = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))

	DropSource = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"), pgrecipe.WithEdit(func(pg *postgres.PgSource) {
		pg.UseFakePrimaryKey = true
	}))
	DropTarget = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	TruncateTarget.Cleanup = model.Truncate
	DropTarget.Cleanup = model.Drop
	helpers.InitSrcDst(helpers.TransferID, &TruncateSource, &TruncateTarget, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, transferID
	helpers.InitSrcDst(helpers.TransferID, &DropSource, &DropTarget, TransferType)         // to WithDefaults() & FillDependentFields(): IsHomo, transferID
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: TruncateSource.Port},
			helpers.LabeledPort{Label: "PG target", Port: TruncateTarget.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Verify", Verify)
		t.Run("Load", Load)
	})
}

func Existence(t *testing.T) {
	_, err := postgres.NewStorage(TruncateSource.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = postgres.NewStorage(TruncateTarget.ToStorageParams())
	require.NoError(t, err)
}

func Verify(t *testing.T) {
	var transfer model.Transfer
	transfer.Src = &DropSource
	transfer.Dst = &DropTarget
	transfer.Type = "SNAPSOT_AND_INCREMENT"

	err := tasks.VerifyDelivery(transfer, logger.Log, helpers.EmptyRegistry())
	require.NoError(t, err)

	dstStorage, err := postgres.NewStorage(DropTarget.ToStorageParams())
	require.NoError(t, err)

	var result bool
	err = dstStorage.Conn.QueryRow(context.Background(), `
		SELECT EXISTS
        (
            SELECT 1
            FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename = '_ping'
        );
	`).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, false, result)
}

func Load(t *testing.T) {
	truncateTransfer := helpers.MakeTransfer(helpers.TransferID, &TruncateSource, &TruncateTarget, TransferType)
	dropTransfer := helpers.MakeTransfer(helpers.TransferID, &DropSource, &DropTarget, TransferType)

	load(t, dropTransfer, true)
	load(t, truncateTransfer, false)
}

func load(t *testing.T, transfer *model.Transfer, updateSource bool) {
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	if updateSource {
		pgSource := transfer.Src.(*postgres.PgSource)
		srcStorage, err := postgres.NewStorage(pgSource.ToStorageParams(nil))
		require.NoError(t, err)
		pushDataToStorage(t, srcStorage)
	}

	//-----------------------------------------------------------------------------------------------------------------

	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_inherited", 10)
	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_inherited_y2006m02", 3)
	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_inherited_y2006m03", 4)
	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_inherited_y2006m04", 3)

	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_declarative", 12)
	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_declarative_y2006m02", 3)
	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_declarative_y2006m03", 4)
	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_declarative_y2006m04", 3)
	helpers.CheckRowsCount(t, transfer.Src, "public", "measurement_declarative_y2006m05", 2)

	sourceStorage := helpers.GetSampleableStorageByModel(t, transfer.Src)
	targetStorage := helpers.GetSampleableStorageByModel(t, transfer.Dst)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m03", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m04", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m03", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m04", sourceStorage, targetStorage, 60*time.Second))
	helpers.CheckRowsCount(t, transfer.Dst, "public", "measurement_inherited", 10)
	helpers.CheckRowsCount(t, transfer.Dst, "public", "measurement_declarative", 12)
	compareParams := helpers.NewCompareStorageParams()
	compareParams.TableFilter = func(tables abstract.TableMap) []abstract.TableDescription {
		return []abstract.TableDescription{
			{
				Name:   "measurement_inherited",
				Schema: "public",
			},
			{
				Name:   "measurement_inherited_y2006m02",
				Schema: "public",
			},
			{
				Name:   "measurement_inherited_y2006m03",
				Schema: "public",
			},
			{
				Name:   "measurement_inherited_y2006m04",
				Schema: "public",
			},
			//skip measurement_declarative because of turned UseFakePrimaryKey option on (limitation of outdated 10.5 PG version)
			{
				Name:   "measurement_declarative_y2006m02",
				Schema: "public",
			},
			{
				Name:   "measurement_declarative_y2006m03",
				Schema: "public",
			},
			{
				Name:   "measurement_declarative_y2006m04",
				Schema: "public",
			},
		}
	}
	require.NoError(t, helpers.CompareStorages(t, transfer.Src, transfer.Dst, compareParams))
}

func pushDataToStorage(t *testing.T, storage *postgres.Storage) {
	//-----------------------------------------------------------------------------------------------------------------
	// update partitioned table created using inheritance directly
	_, err := storage.Conn.Exec(context.Background(), `
        insert into measurement_inherited values
        (6, '2006-02-02', 1),
        (7, '2006-02-02', 1),
        (8, '2006-03-02', 1);
        `)
	require.NoError(t, err)

	_, err = storage.Conn.Exec(context.Background(), `
        update measurement_inherited
        set logdate = '2006-02-10'
        where id = 6;
        `)
	require.NoError(t, err)

	_, err = storage.Conn.Exec(context.Background(), `
        update measurement_inherited
        set logdate = '2006-02-20', id = 8
        where id = 7;
        `)
	require.NoError(t, err)

	_, err = storage.Conn.Exec(context.Background(), `
        delete from measurement_inherited
        where id = 1;
        `)
	require.NoError(t, err)

	//-----------------------------------------------------------------------------------------------------------------
	// update partitioned table created declarartively
	_, err = storage.Conn.Exec(context.Background(), `
        insert into measurement_declarative values
        (6, '2006-02-02', 1),
        (7, '2006-02-02', 1),
        (8, '2006-03-02', 1);
        `)
	require.NoError(t, err)

	_, err = storage.Conn.Exec(context.Background(), `
        update measurement_declarative
        set logdate = '2006-02-10'
        where id = 6;
        `)
	require.NoError(t, err)
	_, err = storage.Conn.Exec(context.Background(), `
        update measurement_declarative
        set logdate = '2006-02-20', id = 8
        where id = 7;
        `)
	require.NoError(t, err)

	_, err = storage.Conn.Exec(context.Background(), `
        delete from measurement_declarative
        where id = 1;
        `)
	require.NoError(t, err)

}
