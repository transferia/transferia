package alltypes

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	postgres_canon "github.com/transferia/transferia/tests/canon/postgres"
	"github.com/transferia/transferia/tests/helpers"
)

func TestAllDataTypes(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Source.WithDefaults()
	Target := pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
	conn, err := provider_postgres.MakeConnPoolFromDst(Target, logger.Log)
	require.NoError(t, err)
	// TODO: Allow to optionally transit extensions as part of transfer
	_, err = conn.Exec(context.Background(), `
create extension if not exists hstore;
create extension if not exists ltree;
create extension if not exists citext;
`)
	require.NoError(t, err)

	helpers.InitSrcDst(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	cases := []string{
		"public.array_types",
		"public.date_types",
		"public.geom_types",
		"public.numeric_types",
		"public.text_types",
		"public.user_types",
		"public.wtf_types",
	}

	tableCase := func(tableName string) func(t *testing.T) {
		return func(t *testing.T) {
			t.Run("initial data", func(t *testing.T) {
				conn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
				require.NoError(t, err)
				_, err = conn.Exec(context.Background(), postgres_canon.TableSQLs[tableName])
				require.NoError(t, err)
			})

			Source.DBTables = []string{tableName}
			transfer := helpers.MakeTransfer(
				t.Name(),
				Source,
				Target,
				abstract.TransferTypeSnapshotAndIncrement,
			)
			transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{tableName}}
			worker := helpers.Activate(t, transfer)

			conn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), postgres_canon.TableSQLs[tableName])
			require.NoError(t, err)
			srcStorage, err := provider_postgres.NewStorage(Source.ToStorageParams(nil))
			require.NoError(t, err)
			dstStorage, err := provider_postgres.NewStorage(Target.ToStorageParams())
			require.NoError(t, err)
			tid, err := abstract.ParseTableIDForProvider(tableName, abstract.ProviderType("pg"))
			require.NoError(t, err)
			require.NoError(t, helpers.WaitEqualRowsCount(t, tid.Namespace, tid.Name, srcStorage, dstStorage, time.Second*30))
			worker.Close(t)
			hashQuery := fmt.Sprintf(`
SELECT md5(array_agg(md5((t.*)::varchar))::varchar)
  FROM (
        SELECT *
          FROM %s
         ORDER BY 1
       ) AS t
;
`, tableName)
			var srcHash string
			require.NoError(t, srcStorage.Conn.QueryRow(context.Background(), hashQuery).Scan(&srcHash))
			var dstHash string
			require.NoError(t, srcStorage.Conn.QueryRow(context.Background(), hashQuery).Scan(&dstHash))
			require.Equal(t, srcHash, dstHash)
		}
	}

	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			t.Run("table", tableCase(c))
		})
	}

	// test fallbacks

	tableCaseAfterFallbackFromCopyFrom := func(tableName string) func(t *testing.T) {
		return func(t *testing.T) {
			t.Run("initial data", func(t *testing.T) {
				conn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
				require.NoError(t, err)
				_, err = conn.Exec(context.Background(), postgres_canon.TableSQLs[tableName])
				require.NoError(t, err)
			})

			Source.DBTables = []string{tableName}
			Target.Cleanup = model.DisabledCleanup
			transfer := helpers.MakeTransfer(
				t.Name(),
				Source,
				Target,
				abstract.TransferTypeSnapshotOnly,
			)
			transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{tableName}}
			_ = helpers.Activate(t, transfer)
		}
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			t.Run("table", tableCaseAfterFallbackFromCopyFrom(c))
		})
	}
}
