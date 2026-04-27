package common_dlq_maker

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func sampleInsertItem(id int64, table string) abstract.ChangeItem {
	schema := abstract.NewTableSchema([]abstract.ColSchema{
		changeitem.NewColSchema("id", ytschema.TypeInt64, true),
		changeitem.NewColSchema("name", ytschema.TypeString, false),
	})
	return abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		Schema:       "public",
		Table:        table,
		ColumnNames:  []string{"id", "name"},
		ColumnValues: []any{id, "alice"},
		TableSchema:  schema,
		ID:           uint32(id),
	}
}

func rowKey(t *testing.T, ci abstract.ChangeItem) []byte {
	t.Helper()
	b, err := json.Marshal(ci.AsMap())
	require.NoError(t, err)
	return b
}

func TestBuildDLQChangeItems_EmptyMalformed(t *testing.T) {
	out, err := BuildDLQChangeItems(
		[]abstract.ChangeItem{sampleInsertItem(1, "t1")},
		nil,
		"_dlq",
		false,
	)
	require.NoError(t, err)
	require.Nil(t, out)
}

func TestBuildDLQChangeItems_ByRowID(t *testing.T) {
	in := []abstract.ChangeItem{sampleInsertItem(1, "t1"), sampleInsertItem(2, "t1")}
	malRow, err := json.Marshal(map[string]any{
		ColNameYqlTransformerRowID: 1,
		"id":                       int64(2),
		"name":                     "alice",
	})
	require.NoError(t, err)
	out, err := BuildDLQChangeItems(in, []MalformedRow{{Row: malRow, Error: "yql err"}}, "_dlq", true)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "t1_dlq", out[0].Table)
	require.Equal(t, uint32(2), out[0].ID)
	require.Equal(t, "yql err", out[0].ColumnValues[len(out[0].ColumnValues)-1])
}

func TestBuildDLQChangeItems_HappyPath(t *testing.T) {
	in := []abstract.ChangeItem{sampleInsertItem(42, "users")}
	malformed := []MalformedRow{
		{Row: rowKey(t, in[0]), Error: "transform failed"},
	}

	out, err := BuildDLQChangeItems(in, malformed, "_dlq", false)
	require.NoError(t, err)
	require.Len(t, out, 1)

	require.Equal(t, "users_dlq", out[0].Table)
	require.Equal(t, uint32(42), out[0].ID)
	require.Contains(t, out[0].ColumnNames, ColNameYqlTransformerErrorMessage)
	require.Equal(t, "transform failed", out[0].ColumnValues[len(out[0].ColumnValues)-1])
	cols := out[0].TableSchema.Columns()
	require.Contains(t, columnNames(cols), ColNameYqlTransformerErrorMessage)
}

func TestBuildDLQChangeItems_RowNotFound_Fatal(t *testing.T) {
	in := []abstract.ChangeItem{sampleInsertItem(1, "t")}
	unknownRow, err := json.Marshal(map[string]any{"id": int64(999), "name": "nobody"})
	require.NoError(t, err)

	_, err = BuildDLQChangeItems(in, []MalformedRow{{Row: unknownRow, Error: "e"}}, "_dlq", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "row")
}

func columnNames(cols []abstract.ColSchema) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, c.ColumnName)
	}
	return out
}
