package generic

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestJSONRest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		input  string
		fields []abstract.ColSchema
		values []interface{}
		rest   string
	}{
		{
			name:  "nested_at_fields",
			input: `{"@fields":{"box":{"dc":"sas","host":"keep"},"trace_id":"trace","other":[1,2]},"extra":true}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "@fields.box.dc", DataType: schema.TypeString.String()},
				{ColumnName: "trace_id", Path: "@fields.trace_id", DataType: schema.TypeString.String()},
			},
			values: []interface{}{"sas", "trace"},
			rest:   `{"@fields":{"box":{"host":"keep"},"other":[1,2]},"extra":true}`,
		},
		{
			name:  "ordinary_paths_and_empty_parents",
			input: `{"fields":{"box":{"dc":"sas"}},"untouched":{}}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.box.dc", DataType: schema.TypeString.String()},
			},
			values: []interface{}{"sas"},
			rest:   `{"untouched":{}}`,
		},
		{
			name:  "slash_paths",
			input: `{"fields":{"dc":"sas","other":"keep"}}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields/dc", DataType: schema.TypeString.String()},
			},
			values: []interface{}{"sas"},
			rest:   `{"fields":{"other":"keep"}}`,
		},
		{
			name:  "missing_and_null_fields",
			input: `{"fields":{"dc":null,"other":"keep"}}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.dc", DataType: schema.TypeString.String()},
				{ColumnName: "missing", Path: "fields.missing", DataType: schema.TypeString.String()},
			},
			values: []interface{}{nil, nil},
			rest:   `{"fields":{"other":"keep"}}`,
		},
		{
			name:  "overlapping_paths_do_not_mutate_column_values",
			input: `{"fields":{"box":{"dc":"sas","host":"keep"},"other":true}}`,
			fields: []abstract.ColSchema{
				{ColumnName: "box", Path: "fields.box", DataType: schema.TypeAny.String()},
				{ColumnName: "dc", Path: "fields.box.dc", DataType: schema.TypeString.String()},
			},
			values: []interface{}{map[string]interface{}{"dc": "sas", "host": "keep"}, "sas"},
			rest:   `{"fields":{"other":true}}`,
		},
		{
			name:  "root_aliases",
			input: `{"@timestamp":"now","msg":"hello","extra":"keep"}`,
			fields: []abstract.ColSchema{
				{ColumnName: "timestamp", Path: "@timestamp", DataType: schema.TypeString.String()},
				{ColumnName: "message", Path: "msg", DataType: schema.TypeString.String()},
			},
			values: []interface{}{"now", "hello"},
			rest:   `{"extra":"keep"}`,
		},
		{
			name:  "nested_column_before_parent",
			input: `{"fields":{"box":{"dc":"sas","host":"keep"},"other":true}}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.box.dc", DataType: schema.TypeString.String()},
				{ColumnName: "box", Path: "fields.box", DataType: schema.TypeAny.String()},
			},
			values: []interface{}{"sas", map[string]interface{}{"dc": "sas", "host": "keep"}},
			rest:   `{"fields":{"other":true}}`,
		},
		{
			name:  "two_columns_from_same_path",
			input: `{"fields":{"dc":"sas"}}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.dc", DataType: schema.TypeString.String()},
				{ColumnName: "dc_copy", Path: "fields.dc", DataType: schema.TypeString.String()},
			},
			values: []interface{}{"sas", "sas"},
			rest:   `{}`,
		},
		{
			name:  "missing_path_preserves_empty_object",
			input: `{"fields":{}}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.dc", DataType: schema.TypeString.String()},
			},
			values: []interface{}{nil},
			rest:   `{"fields":{}}`,
		},
		{
			name:  "invalid_ancestor_is_preserved",
			input: `{"fields":"not json"}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.dc", DataType: schema.TypeString.String()},
			},
			values: []interface{}{nil},
			rest:   `{"fields":"not json"}`,
		},
		{
			name:  "json_encoded_object",
			input: `{"fields":"{\"dc\":\"sas\",\"other\":\"keep\"}"}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.dc", DataType: schema.TypeString.String()},
			},
			values: []interface{}{"sas"},
			rest:   `{"fields":"{\"other\":\"keep\"}"}`,
		},
		{
			name:  "json_encoded_number_keeps_precision",
			input: `{"fields":"{\"dc\":\"sas\",\"number\":9007199254740993}"}`,
			fields: []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.dc", DataType: schema.TypeString.String()},
			},
			values: []interface{}{"sas"},
			rest:   `{"fields":"{\"number\":9007199254740993}"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &GenericParserConfig{Format: "json", Fields: tc.fields, AuxOpts: AuxParserOpts{AddRest: true, AddDedupeKeys: true}}
			parser := NewGenericParser(cfg, tc.fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
			items := parser.Do(makePersqueueReadMessage(0, tc.input), abstract.Partition{Topic: "test"})
			require.Len(t, items, 1)
			require.Equal(t, "test", items[0].Table)
			require.Equal(t, tc.values, items[0].ColumnValues[:len(tc.fields)])
			rest, err := json.Marshal(items[0].ColumnValues[len(tc.fields)])
			require.NoError(t, err)
			require.JSONEq(t, tc.rest, string(rest))
		})
	}
}

func TestJSONRestEvents(t *testing.T) {
	for _, tc := range []struct {
		format string
		input  string
		rest   string
	}{
		{format: "json", input: `{"fields":{"dc":"sas","extra":"keep"}}`, rest: `{"fields":{"extra":"keep"}}`},
		{format: "tskv", input: "fields={\"dc\":\"sas\",\"extra\":\"keep\"}", rest: `{"fields":"{\"extra\":\"keep\"}"}`},
	} {
		t.Run(tc.format, func(t *testing.T) {
			fields := []abstract.ColSchema{
				{ColumnName: "dc", Path: "fields.dc", DataType: schema.TypeString.String()},
			}
			cfg := &GenericParserConfig{Format: tc.format, Fields: fields, AuxOpts: AuxParserOpts{AddRest: true, AddDedupeKeys: true}}
			parser := NewGenericParser(cfg, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
			batch := parser.Parse(makePersqueueReadMessage(0, tc.input), abstract.Partition{Topic: "test"})
			require.Equal(t, 1, batch.Count())
			require.True(t, batch.Next())
			event, err := batch.Event()
			require.NoError(t, err)
			legacy, ok := event.(abstract2.SupportsOldChangeItem)
			require.True(t, ok)
			item, err := legacy.ToOldChangeItem()
			require.NoError(t, err)
			require.Equal(t, "sas", item.ColumnValues[0])
			rest, err := json.Marshal(item.ColumnValues[1])
			require.NoError(t, err)
			require.JSONEq(t, tc.rest, string(rest))
		})
	}
}
