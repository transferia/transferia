//go:build !disable_clickhouse_provider

package clickhouse

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	_ "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/parsers/tests/samples"
	"github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/tests/canon"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	_ = os.Setenv("CH_LOCAL_PATH", os.Getenv("RECIPE_CLICKHOUSE_BIN"))
}

func TestSqlTransformer_Apply(t *testing.T) {
	t.Run("exclude tables with no PKey", func(t *testing.T) {
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query: `
select
	id,
	val,
	cityHash64(val) as hashed_title
from table;
`,
		}, logger.Log)
		require.NoError(t, err)
		_, err = transformer.ResultSchema(abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String()},
			{ColumnName: "val", DataType: ytschema.TypeAny.String()},
		}))
		require.Error(t, err)
	})
	t.Run("valid query", func(t *testing.T) {
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query: `
select
	id,
	val,
	cityHash64(val) as hashed_title
from table;
`,
		}, logger.Log)
		require.NoError(t, err)
		require.True(t, transformer.Suitable(abstract.TableID{Name: "test"}, abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "val", DataType: ytschema.TypeAny.String()},
		})))
	})
	t.Run("invalid query", func(t *testing.T) {
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query:  "selet *, 1+1 as res from table",
		}, logger.Log)
		require.NoError(t, err)
		_, err = transformer.ResultSchema(abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "val", DataType: ytschema.TypeAny.String()},
		}))
		require.Error(t, err)
	})
	t.Run("table_schema", func(t *testing.T) {
		inputSchema := []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "val", DataType: ytschema.TypeAny.String()},
		}
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query:  "select *, (1+1) as res from table",
		}, logger.Log)
		require.NoError(t, err)
		require.True(t, transformer.Suitable(abstract.TableID{Name: "test"}, abstract.NewTableSchema(inputSchema)))
		resSchema, _ := transformer.ResultSchema(abstract.NewTableSchema(inputSchema))
		require.Len(t, resSchema.Columns(), 3)
		require.True(t, resSchema.Columns().HasPrimaryKey())
	})
	t.Run("actual_data", func(t *testing.T) {
		inputSchema := []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "val", DataType: ytschema.TypeAny.String()},
		}
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query: `
select
    id,
    val,
    toInt8(id+1) as res
from table`,
		}, logger.Log)
		require.NoError(t, err)
		require.True(t, transformer.Suitable(abstract.TableID{Name: "test_table"}, abstract.NewTableSchema(inputSchema)))
		resSchema, _ := transformer.ResultSchema(abstract.NewTableSchema(inputSchema))
		require.Len(t, resSchema.Columns(), 3)
		require.True(t, resSchema.Columns().HasPrimaryKey())
		transformed := transformer.Apply([]abstract.ChangeItem{
			abstract.ChangeItemFromMap(
				map[string]interface{}{
					"id":  1,
					"val": "part",
				},
				abstract.NewTableSchema(inputSchema),
				"test_table",
				"insert",
			),
		})
		require.Len(t, transformed.Errors, 0)
		require.Len(t, transformed.Transformed, 1)
		require.Len(t, transformed.Transformed[0].ColumnValues, 3)
		require.Equal(t, transformed.Transformed[0].ColumnValues, []interface{}{int32(1), "part", int8(2)})
	})
	t.Run("collapse, single delete", func(t *testing.T) {
		all := canon.AllReplicationSequences()
		abstract.Dump(all[0].Items)
		masterChangeItem := all[0].Items[0]
		inputSchema := masterChangeItem.TableSchema
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query:  `select * from table where i1 in (1, 2) and i2 in (2, 3)`,
		}, logger.Log)
		require.NoError(t, err)
		require.True(t, transformer.Suitable(masterChangeItem.TableID(), inputSchema))
		resSchema, _ := transformer.ResultSchema(inputSchema)
		require.Len(t, resSchema.Columns(), 3)

		transformed := transformer.Apply(canon.AllReplicationSequences()[0].Items)
		require.Len(t, transformed.Errors, 0)
		abstract.Dump(transformed.Transformed)
		require.Len(t, transformed.Transformed, 3)
		require.Len(t, transformed.Transformed[0].ColumnValues, 3)
		require.Equal(t, transformed.Transformed[2].Kind, abstract.DeleteKind)
		require.Len(t, transformed.Transformed[2].OldKeys.KeyValues, 3)
	})
	t.Run("insert update insert", func(t *testing.T) {
		testData := canon.AllReplicationSequences()[2]
		masterChangeItem := testData.Items[0]
		inputSchema := masterChangeItem.TableSchema
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query:  `select * from table where i2 in (1, 2)`,
		}, logger.Log)
		require.NoError(t, err)
		require.True(t, transformer.Suitable(masterChangeItem.TableID(), inputSchema))
		resSchema, _ := transformer.ResultSchema(inputSchema)
		require.Len(t, resSchema.Columns(), 3)

		abstract.Dump(testData.Items)
		transformed := transformer.Apply(testData.Items)
		abstract.Dump(transformed.Transformed)
		require.Len(t, transformed.Errors, 0)
		require.Len(t, transformed.Transformed, 6)
		require.Len(t, transformed.Transformed[0].ColumnValues, 3)
		require.Equal(t, transformed.Transformed[0].Kind, abstract.InsertKind)
		require.Equal(t, transformed.Transformed[3].Kind, abstract.DeleteKind)
		require.Len(t, transformed.Transformed[3].OldKeys.KeyValues, 3)
		require.Equal(t, transformed.Transformed[4].Kind, abstract.InsertKind)
	})
	t.Run("append-only data", func(t *testing.T) {
		parser, err := parsers.NewParserFromMap(parserConfigMap(samples.JSONSample), false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		require.NoError(t, err)
		inputData := samples.Data[samples.JSONSample]
		inputData.CreateTime = time.Now().UTC()
		inputData.WriteTime = time.Now().Add(-time.Second).UTC()
		resArr := parser.Do(inputData, abstract.Partition{})
		require.NotEmpty(t, resArr)
		masterChangeItem := resArr[0]
		inputSchema := masterChangeItem.TableSchema
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query:  `select *, 1 as extra from table`,
		}, logger.Log)
		require.NoError(t, err)
		require.True(t, transformer.Suitable(masterChangeItem.TableID(), inputSchema))
		resSchema, _ := transformer.ResultSchema(inputSchema)
		require.Len(t, resSchema.Columns(), len(inputSchema.Columns())+1)
		transformed := transformer.Apply(resArr)
		abstract.Dump(transformed.Transformed)
		require.Equal(t, len(transformed.Errors), 0)
		require.Equal(t, len(transformed.Transformed), len(resArr))
	})
	t.Run("exec-dict", func(t *testing.T) {
		inputSchema := []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "val", DataType: ytschema.TypeAny.String()},
		}
		transformer, err := New(Config{
			Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
			Query: `
	CREATE
		DICTIONARY pwn (k String, v String) primary key k
		SOURCE(executable(command 'echo "1,$(whoami)"' format 'CSV')) layout (flat()) lifetime (0);
	select 1 as id, dictGet('pwn', 'v', 1) as pwn`,
		}, logger.Log)
		require.NoError(t, err)
		require.True(t, transformer.Suitable(abstract.TableID{Name: "test"}, abstract.NewTableSchema(inputSchema)))
		resSchema, err := transformer.ResultSchema(abstract.NewTableSchema(inputSchema))
		require.Len(t, resSchema.Columns(), 0)
		require.Error(t, err)
	})
}

func parserConfigMap(name string) map[string]interface{} {
	var source kafka.KafkaSource
	_ = json.Unmarshal([]byte(samples.Configs[name]), &source)
	source.WithDefaults()
	return source.ParserConfig
}
