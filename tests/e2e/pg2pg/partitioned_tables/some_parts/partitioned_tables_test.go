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
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/yatestx"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir(yatestx.ProjectSource("dump")), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(
		"public.measurement_inherited",
		"public.measurement_inherited_y2006m02",
		"public.measurement_inherited_y2006m04",
		"public.measurement_declarative",
		"public.measurement_declarative_y2006m02",
		"public.measurement_declarative_y2006m04",
	), pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
		pg.UseFakePrimaryKey = true
	}))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, transferID
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Verify", Verify)
		t.Run("Load", Load)
	})
}

func Existence(t *testing.T) {
	_, err := provider_postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = provider_postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Verify(t *testing.T) {
	var transfer model.Transfer
	transfer.Src = &Source
	transfer.Dst = &Target
	transfer.Type = "SNAPSOT_AND_INCREMENT"

	err := tasks.VerifyDelivery(context.Background(), transfer, logger.Log, testmetrics.EmptyRegistry())
	require.NoError(t, err)

	dstStorage, err := provider_postgres.NewStorage(Target.ToStorageParams())
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

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	srcStorage, err := provider_postgres.NewStorage(Source.ToStorageParams(nil))
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

	storagecomparison.CheckRowsCount(t, Source, "public", "measurement_inherited", 10)
	storagecomparison.CheckRowsCount(t, Source, "public", "measurement_inherited_y2006m02", 3)
	storagecomparison.CheckRowsCount(t, Source, "public", "measurement_inherited_y2006m03", 4)
	storagecomparison.CheckRowsCount(t, Source, "public", "measurement_inherited_y2006m04", 3)
	storagecomparison.CheckRowsCount(t, Source, "public", "measurement_declarative", 10)
	storagecomparison.CheckRowsCount(t, Source, "public", "measurement_declarative_y2006m02", 3)
	storagecomparison.CheckRowsCount(t, Source, "public", "measurement_declarative_y2006m03", 4)
	storagecomparison.CheckRowsCount(t, Source, "public", "measurement_declarative_y2006m04", 3)

	sourceStorage := storagecomparison.GetSampleableStorageByModel(t, Source)
	targetStorage := storagecomparison.GetSampleableStorageByModel(t, Target)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_inherited_y2006m04", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m02", sourceStorage, targetStorage, 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "measurement_declarative_y2006m04", sourceStorage, targetStorage, 60*time.Second))
	storagecomparison.CheckRowsCount(t, Target, "public", "measurement_inherited", 6)
	storagecomparison.CheckRowsCount(t, Target, "public", "measurement_declarative", 6)
	compareParams := storagecomparison.NewCompareStorageParams()
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
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, compareParams))
}
