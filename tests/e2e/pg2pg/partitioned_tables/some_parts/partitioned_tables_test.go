package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(
		"public.measurement_inherited",
		"public.measurement_inherited_y2006m02",
		"public.measurement_inherited_y2006m04",
		"public.measurement_declarative",
		"public.measurement_declarative_y2006m02",
		"public.measurement_declarative_y2006m04",
	), pgrecipe.WithEdit(func(pg *postgres.PgSource) {
		pg.UseFakePrimaryKey = true
	}))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, transferID
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Verify", Verify)
		t.Run("Load", Load)
	})
}

func Existence(t *testing.T) {
	_, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Verify(t *testing.T) {
	var transfer model.Transfer
	transfer.Src = &Source
	transfer.Dst = &Target
	transfer.Type = "SNAPSOT_AND_INCREMENT"

	err := tasks.VerifyDelivery(transfer, logger.Log, helpers.EmptyRegistry())
	require.NoError(t, err)

	dstStorage, err := postgres.NewStorage(Target.ToStorageParams())
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
	Source.PreSteps.Rule = false // if true then all rules will been tried to transfer even rules for excluded partitions

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	srcStorage, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)

	//-----------------------------------------------------------------------------------------------------------------
	_, err = srcStorage.Conn.Exec(context.Background(), `
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

	//-----------------------------------------------------------------------------------------------------------------
	_, err = srcStorage.Conn.Exec(context.Background(), `
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

	//-----------------------------------------------------------------------------------------------------------------

	helpers.CheckRowsCount(t, Source, "public", "measurement_inherited", 10)
	helpers.CheckRowsCount(t, Source, "public", "measurement_inherited_y2006m02", 3)
	helpers.CheckRowsCount(t, Source, "public", "measurement_inherited_y2006m03", 4)
	helpers.CheckRowsCount(t, Source, "public", "measurement_inherited_y2006m04", 3)
	helpers.CheckRowsCount(t, Source, "public", "measurement_declarative", 10)
	helpers.CheckRowsCount(t, Source, "public", "measurement_declarative_y2006m02", 3)
	helpers.CheckRowsCount(t, Source, "public", "measurement_declarative_y2006m03", 4)
	helpers.CheckRowsCount(t, Source, "public", "measurement_declarative_y2006m04", 3)

	sourceStorage := helpers.GetSampleableStorageByModel(t, Source)
	targetStorage := helpers.GetSampleableStorageByModel(t, Target)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m04", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m04", sourceStorage, targetStorage, 60*time.Second))
	helpers.CheckRowsCount(t, Target, "public", "measurement_inherited", 6)
	helpers.CheckRowsCount(t, Target, "public", "measurement_declarative", 6)
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
				Name:   "measurement_inherited_y2006m04",
				Schema: "public",
			},
			// skip measurement_declarative because of turned UseFakePrimaryKey option on (limitation of outdated 10.5 PG version)
			{
				Name:   "measurement_declarative_y2006m02",
				Schema: "public",
			},
			{
				Name:   "measurement_declarative_y2006m04",
				Schema: "public",
			},
		}
	}
	require.NoError(t, helpers.CompareStorages(t, Source, Target, compareParams))
}
