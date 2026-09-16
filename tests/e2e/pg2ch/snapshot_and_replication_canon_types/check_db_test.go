package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	postgres_canon "github.com/transferia/transferia/tests/canon/postgres"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
)

func TestSnapshotAndIncrement(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga

	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Target := chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
	t.Setenv("YC", "1")                                                                  // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	tableCase := func(tableName string) func(t *testing.T) {
		return func(t *testing.T) {
			tid, err := abstract.ParseTableIDForProvider(tableName, abstract.ProviderType("pg"))
			require.NoError(t, err)
			conn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), fmt.Sprintf(`drop table if exists %s`, tableName))
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), postgres_canon.TableSQLs[tableName])
			require.NoError(t, err)

			transfer := transferhelpers.MakeTransfer(
				tableName,
				Source,
				Target,
				abstract.TransferTypeSnapshotAndIncrement,
			)
			transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{tableName}}
			worker := delivery.Activate(t, transfer)

			conn, err = provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), postgres_canon.TableSQLs[tableName])
			require.NoError(t, err)
			require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, tid.Name, storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
			require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator).WithPriorityComparators(pg2ch.ValueComparator)))
			defer worker.Close(t)
		}
	}
	// t.Run("array_types", tableCase("public.array_types"))
	t.Run("date_types", tableCase("public.date_types"))
	t.Run("geom_types", tableCase("public.geom_types"))
	t.Run("numeric_types", tableCase("public.numeric_types"))
	t.Run("text_types", tableCase("public.text_types"))
	// t.Run("wtf_types", tableCase("public.wtf_types"))
}
