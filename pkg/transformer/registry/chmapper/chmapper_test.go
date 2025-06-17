//go:build !disable_clickhouse_provider

package chmapper

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/parsers"
	_ "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/parsers/tests/samples"
	"github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestSimpleRemap(t *testing.T) {
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
		transformer := New(Config{
			Table: TableRemap{
				OriginalTable: abstract.TableID{},
				RenamedTable: abstract.TableID{
					Namespace: "test_schema",
					Name:      "test_name",
				},
				Columns: []ColumnRemap{
					{Name: "resource", Path: "cluster_id", Key: true, Type: ytschema.TypeBytes.String(), TargetType: "LowCardinality(String)"},
					{Name: "cluster_name", Type: ytschema.TypeBytes.String(), TargetType: "LowCardinality(String)"},
					{Name: "host", Type: ytschema.TypeBytes.String(), TargetType: "LowCardinality(String)"},
					{Name: "database", Type: ytschema.TypeBytes.String(), TargetType: "LowCardinality(String)"},
					{Name: "pid", Type: ytschema.TypeUint32.String()},
					{Name: "version", Type: ytschema.TypeUint64.String(), Excluded: true},
					{Name: "_rest", Type: ytschema.TypeAny.String(), Excluded: true},
					{Name: "_timestamp", Type: ytschema.TypeTimestamp.String(), TargetType: "DateTime(6)"},
					{Name: "_partition", Type: ytschema.TypeString.String(), TargetType: "LowCardinality(String)", Masked: true},
					{Name: "_offset", Type: ytschema.TypeUint64.String()},
					{Name: "_idx", Type: ytschema.TypeUint32.String()},
				},
				DDL: &DDLParams{
					PartitionBy: "toDate(_timestamp)",
					OrderBy:     "resource, _timestamp",
					Engine:      "ReplicatedMergeTree('/clickhouse/tables/{shard}/test_schema_test_table')",
					TTL:         "_timestamp + INTERVAL 7 day",
				},
			},
			Salt: "test",
		})
		require.NoError(t, err)
		fmt.Println(transformer.Description())
		require.True(t, transformer.Suitable(masterChangeItem.TableID(), inputSchema))
		resSchema, _ := transformer.ResultSchema(inputSchema)
		require.Len(t, resSchema.Columns(), len(inputSchema.Columns())-2)
		ddlTransformed := transformer.Apply(
			[]changeitem.ChangeItem{
				{
					Kind:        changeitem.InitShardedTableLoad,
					Schema:      resArr[0].Schema,
					Table:       resArr[0].Table,
					TableSchema: resArr[0].TableSchema,
				},
			},
		)
		fmt.Printf("%v\n\n", ddlTransformed.Transformed[0].ColumnValues[0])

		transformed := transformer.Apply(resArr)
		abstract.Dump(transformed.Transformed)
		require.Equal(t, len(transformed.Errors), 0)
		require.Equal(t, len(transformed.Transformed), len(resArr))
	})
}

func parserConfigMap(name string) map[string]interface{} {
	var source kafka.KafkaSource
	_ = json.Unmarshal([]byte(samples.Configs[name]), &source)
	source.WithDefaults()
	return source.ParserConfig
}
