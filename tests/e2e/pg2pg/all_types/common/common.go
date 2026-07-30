package common

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	postgres_canon "github.com/transferia/transferia/tests/canon/postgres"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/serde"
	helpers_transformer "github.com/transferia/transferia/tests/helpers/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestAllDataTypes(t *testing.T, source *provider_postgres.PgSource, target *provider_postgres.PgDestination) {
	conn, err := provider_postgres.MakeConnPoolFromDst(target, logger.Log)
	require.NoError(t, err)
	defer conn.Close()
	// TODO: Allow to optionally transit extensions as part of transfer
	_, err = conn.Exec(context.Background(), `
create extension if not exists hstore;
create extension if not exists ltree;
create extension if not exists citext;
`)
	require.NoError(t, err)

	helpers.InitSrcDst(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)

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
				conn, err := provider_postgres.MakeConnPoolFromSrc(source, logger.Log)
				require.NoError(t, err)
				defer conn.Close()
				_, err = conn.Exec(context.Background(), postgres_canon.TableSQLs[tableName])
				require.NoError(t, err)
			})

			source.DBTables = []string{tableName}
			transfer := helpers.MakeTransfer(
				t.Name(),
				source,
				target,
				abstract.TransferTypeSnapshotAndIncrement,
			)
			transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{tableName}}
			worker := helpers.Activate(t, transfer)

			conn, err := provider_postgres.MakeConnPoolFromSrc(source, logger.Log)
			require.NoError(t, err)
			defer conn.Close()
			_, err = conn.Exec(context.Background(), postgres_canon.TableSQLs[tableName])
			require.NoError(t, err)
			srcStorage, err := provider_postgres.NewStorage(source.ToStorageParams(nil))
			require.NoError(t, err)
			defer srcStorage.Close()
			dstStorage, err := provider_postgres.NewStorage(target.ToStorageParams())
			require.NoError(t, err)
			defer dstStorage.Close()
			tid, err := abstract.ParseTableIDForProvider(tableName, abstract.ProviderType("pg"))
			require.NoError(t, err)
			require.NoError(t, helpers.WaitEqualRowsCount(t, tid.Namespace, tid.Name, srcStorage, dstStorage, time.Second*30))
			worker.Close(t)

			// Log per-row JSON representation to spot differences
			logTableDataJSON := func(label string, conn *pgxpool.Pool) string {
				rows, err := conn.Query(context.Background(),
					fmt.Sprintf("SELECT __primary_key, to_jsonb(t)::text AS row_text FROM %s AS t ORDER BY __primary_key", tableName))
				require.NoError(t, err)
				defer rows.Close()
				var data []string
				for rows.Next() {
					var pk int
					var rowText string
					require.NoError(t, rows.Scan(&pk, &rowText))
					data = append(data, fmt.Sprintf("pk=%d: %s", pk, rowText))
				}
				logger.Log.Info(label+" table rows as JSON", log.String("table", tableName), log.Any("rows", data))
				return strings.Join(data, ",")
			}
			src := logTableDataJSON(fmt.Sprintf("source_under_md5 for table:%s", tableName), srcStorage.Conn)
			dst := logTableDataJSON(fmt.Sprintf("destination_under_md5 for table:%s", tableName), dstStorage.Conn)

			if tableName == "public.numeric_types" { // bcs we can save '0.0' (src) as '0' (dst)
				src = strings.ReplaceAll(src, `"t_numeric": 0.0,`, `"t_numeric": 0,`)
				dst = strings.ReplaceAll(dst, `"t_numeric": 0.0,`, `"t_numeric": 0,`)
			}
			require.Equal(t, src, dst)

			if tableName == "public.numeric_types" { // bcs we can save '0.0' (src) as '0' (dst) -- then their hash is differs
				return
			}

			hashQuery := fmt.Sprintf(`
SELECT md5(
    string_agg(
        md5(to_jsonb(t)::text),
        '' ORDER BY __primary_key
    )
)
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
			require.NoError(t, dstStorage.Conn.QueryRow(context.Background(), hashQuery).Scan(&dstHash))
			require.Equal(t, srcHash, dstHash)

		}
	}

	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			t.Run("table", tableCase(c))
		})
	}

	// test fallbacks

	queriesStr := make([]string, 0)
	tableCaseAfterFallbackFromCopyFrom := func(tableName string) func(t *testing.T) {
		return func(t *testing.T) {
			t.Run("initial data", func(t *testing.T) {
				conn, err := provider_postgres.MakeConnPoolFromSrc(source, logger.Log)
				require.NoError(t, err)
				defer conn.Close()
				_, err = conn.Exec(context.Background(), postgres_canon.TableSQLs[tableName])
				require.NoError(t, err)
			})

			source.DBTables = []string{tableName}
			target.Cleanup = model.DisabledCleanup
			transfer := helpers.MakeTransfer(
				t.Name(),
				source,
				target,
				abstract.TransferTypeSnapshotOnly,
			)
			transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{tableName}}

			changeItems := make([]abstract.ChangeItem, 0)
			handler := func(t *testing.T, in []abstract.ChangeItem) abstract.TransformerResult {
				changeItems = append(changeItems, in...)
				return abstract.TransformerResult{
					Transformed: in,
					Errors:      nil,
				}
			}
			debeziumSerDeTransformer := helpers_transformer.NewSimpleTransformer(t, handler, serde.AnyTablesUdf)
			require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))
			_ = helpers.Activate(t, transfer)

			// check
			queryFilter := make([]abstract.ChangeItem, 0)
			for _, currChangeItem := range changeItems {
				fmt.Printf("QQQ::CHANGE_ITEM::%s\n", currChangeItem.ToJSONString())
				if currChangeItem.IsRowEvent() {
					queryFilter = append(queryFilter, currChangeItem)
				}
			}
			queries, err := provider_postgres.BuildBulkInsertQuery("my_table", queryFilter[0].TableSchema.Columns(), nil, 1024*1024, queryFilter)
			require.NoError(t, err)

			for _, currQuery := range queries {
				currQueryStr := normalize(fmt.Sprintf("%v", currQuery))
				fmt.Printf("QQQ::query::%v\n", currQuery)
				queriesStr = append(queriesStr, currQueryStr)
			}
		}
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			t.Run("table", tableCaseAfterFallbackFromCopyFrom(c))
		})
	}
	canon.SaveJSON(t, queriesStr)

	// Also test with text serialization format to catch regressions
	// in the text representation path (e.g. *pgtype.GenericText handling
	// for extension types like ltree, citext).
	t.Run("text_format", func(t *testing.T) {
		source.SnapshotSerializationFormat = provider_postgres.PgSerializationFormatText
		for _, c := range cases {
			t.Run(c, func(t *testing.T) {
				t.Run("table", tableCase(c))
			})
		}
	})
}

//------------------------------------------------------------------------------------------------

// hstore can be:
// - a=>1,b=>2
// - b=>2,a=>1

var pairsRe = regexp.MustCompile(`[a-zA-Z0-9_]+=>[a-zA-Z0-9_]+(?:,[a-zA-Z0-9_]+=>[a-zA-Z0-9_]+)+`)

func sortPairs(s string) string {
	parts := strings.Split(s, ",")

	sort.Slice(parts, func(i, j int) bool {
		ki := strings.SplitN(parts[i], "=>", 2)[0]
		kj := strings.SplitN(parts[j], "=>", 2)[0]
		return ki < kj
	})

	return strings.Join(parts, ",")
}

func normalize(text string) string {
	return pairsRe.ReplaceAllStringFunc(text, sortPairs)
}
