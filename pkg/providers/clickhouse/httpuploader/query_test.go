//go:build !disable_clickhouse_provider

package httpuploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	RowCount = 500
	Attempts = 10
)

func prepareTestData() []abstract.ChangeItem {
	colSchema := abstract.NewTableSchema([]abstract.ColSchema{
		abstract.NewColSchema("id", schema.TypeInt32, true),
		abstract.NewColSchema("value", schema.TypeString, true),
	})
	colNames := []string{"id", "value"}
	batch := make([]abstract.ChangeItem, 0, RowCount)
	for i := 0; i < RowCount; i++ {
		batch = append(batch, abstract.ChangeItem{
			ID:           0,
			LSN:          0,
			CommitTime:   0,
			Counter:      0,
			Kind:         abstract.InsertKind,
			Schema:       "default",
			Table:        "test",
			ColumnNames:  colNames,
			ColumnValues: []interface{}{i, fmt.Sprintf("Text data %d", i)},
			TableSchema:  colSchema,
			OldKeys:      abstract.OldKeysType{},
			TxID:         "",
			Query:        "",
		})
	}
	return batch
}

func testQueryStableMarshalling(t *testing.T, input []abstract.ChangeItem) {
	q := newInsertQuery(model.InsertParams{MaterializedViewsIgnoreErrors: false}, input[0].Schema, input[0].Table, 500, new(sync.Pool))
	defer q.Close()
	colTypes := columntypes.TypeMapping{
		"id":    columntypes.NewTypeDescription("Int32"),
		"value": columntypes.NewTypeDescription("String"),
	}
	require.NoError(t, marshalQuery(input, NewRules(
		input[0].ColumnNames,
		input[0].TableSchema.Columns(),
		abstract.MakeMapColNameToIndex(input[0].TableSchema.Columns()),
		colTypes,
		false,
	), q, 30, 10))
	data, err := io.ReadAll(q)
	require.NoError(t, err)

	rows := bytes.Split(data, []byte{'\n'})[1:]
	rows = rows[:len(rows)-1] // Remove last empty slice after split by '\n'
	require.Len(t, rows, RowCount)

	type dataRow struct {
		ID    int    `json:"id"`
		Value string `json:"value"`
	}

	for idx, row := range rows {
		var parsedRow dataRow
		require.NoError(t, json.Unmarshal(row, &parsedRow))
		require.Equal(t, idx, parsedRow.ID)
	}
}

func TestQueryStableMarshalling(t *testing.T) {
	input := prepareTestData()

	for i := 0; i < Attempts; i++ {
		testQueryStableMarshalling(t, input)
	}
}

func TestQueryBool(t *testing.T) {
	changeItemStr := `
	{
		"id": 0,
		"nextlsn": 1,
		"commitTime": 1663054970837000000,
		"txPosition": 0,
		"kind": "insert",
		"schema": "",
		"table": "mdb_porto_test_rt_perf_diag_pg_stat_statements",
		"columnnames": ["collect_time", "toplevel"],
		"columnvalues": ["2022-09-13T10:42:49+03:00", true],
		"table_schema": [{
			"path": "",
			"name": "collect_time",
			"type": "datetime",
			"key": true,
			"required": false,
			"original_type": ""
		}, {
			"path": "",
			"name": "toplevel",
			"type": "boolean",
			"key": false,
			"required": false,
			"original_type": ""
		}],
		"oldkeys": {},
		"tx_id": "",
		"query": ""
	}
	`
	changeItem, _ := abstract.UnmarshalChangeItem([]byte(changeItemStr))

	colTypes := make(columntypes.TypeMapping)
	typeDescription := columntypes.TypeDescription{}
	colTypes["collect_time"] = &typeDescription
	colTypes["toplevel"] = &typeDescription

	var buf bytes.Buffer
	err := MarshalCItoJSON(*changeItem, NewRules(
		changeItem.ColumnNames,
		changeItem.TableSchema.Columns(),
		abstract.MakeMapColNameToIndex(changeItem.TableSchema.Columns()),
		colTypes,
		true,
	), &buf)
	require.NoError(t, err)

	var q map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &q)
	require.NoError(t, err)

	v, ok := q["toplevel"]
	require.True(t, ok)
	require.Equal(t, "bool", fmt.Sprintf("%T", v))
}

func TestOrder(t *testing.T) {
	changeItemStr := `
	{
		"id": 0,
		"nextlsn": 1,
		"commitTime": 1663054970837000000,
		"txPosition": 0,
		"kind": "insert",
		"schema": "",
		"table": "mdb_porto_test_rt_perf_diag_pg_stat_statements",
		"columnnames": ["toplevel", "collect_time"],
		"columnvalues": [true, "2022-09-13T10:42:49+03:00"],
		"table_schema": [{
			"path": "",
			"name": "collect_time",
			"type": "datetime",
			"key": true,
			"required": false,
			"original_type": ""
		}, {
			"path": "",
			"name": "toplevel",
			"type": "boolean",
			"key": false,
			"required": false,
			"original_type": ""
		}],
		"oldkeys": {},
		"tx_id": "",
		"query": ""
	}
	`
	changeItem, _ := abstract.UnmarshalChangeItem([]byte(changeItemStr))

	colTypes := make(columntypes.TypeMapping)
	typeDescription := columntypes.TypeDescription{}
	colTypes["collect_time"] = &typeDescription
	colTypes["toplevel"] = &typeDescription

	var buf bytes.Buffer
	err := MarshalCItoJSON(*changeItem, NewRules(
		changeItem.ColumnNames,
		changeItem.TableSchema.Columns(),
		abstract.MakeMapColNameToIndex(changeItem.TableSchema.Columns()),
		colTypes,
		true,
	), &buf)
	require.NoError(t, err)

	var q map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &q)
	require.NoError(t, err)

	v, ok := q["toplevel"]
	require.True(t, ok)
	require.True(t, v.(bool))
}
