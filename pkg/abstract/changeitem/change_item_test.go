package changeitem

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/test/canon"
	"go.ytsaurus.tech/yt/go/yson"
)

var (
	testChangeItem = ChangeItem{
		ID:           100,
		LSN:          200,
		CommitTime:   1961,
		Counter:      9,
		Kind:         InsertKind,
		Schema:       "schema",
		Table:        "table",
		PartID:       "part_id",
		ColumnNames:  []string{"a", "b"},
		ColumnValues: []any{"av", "bv"},
		TableSchema: NewTableSchema([]ColSchema{
			{
				TableSchema:  "schema",
				TableName:    "table",
				Path:         "path",
				ColumnName:   "a",
				DataType:     "data",
				PrimaryKey:   true,
				FakeKey:      true,
				Required:     true,
				Expression:   "expression",
				OriginalType: "orig",
				Properties: map[PropertyKey]any{
					"k": "v",
				},
			},
			{
				TableSchema:  "schema",
				TableName:    "table",
				Path:         "path",
				ColumnName:   "b",
				DataType:     "data",
				PrimaryKey:   true,
				FakeKey:      true,
				Required:     true,
				Expression:   "expression",
				OriginalType: "orig",
				Properties: map[PropertyKey]any{
					"x": "y",
				},
			},
		}),
		OldKeys: OldKeysType{
			KeyNames:  []string{"a", "b"},
			KeyTypes:  []string{"at", "bt"},
			KeyValues: []any{"av", "bv"},
		},
		TxID:  "tx_id",
		Query: "query",
	}

	changeItemJSON = `{
		"id": 100,
		"nextlsn": 200,
		"commitTime": 1961,
		"txPosition": 9,
		"kind": "insert",
		"schema": "schema",
		"table": "table",
		"part": "part_id",
		"columnnames": [
			"a",
			"b"
		],
		"columnvalues": [
			"av",
			"bv"
		],
		"table_schema": [
			{
				"table_schema": "schema",
				"table_name": "table",
				"path": "path",
				"name": "a",
				"type": "data",
				"key": true,
				"fake_key": true,
				"required": true,
				"expression": "expression",
				"system_key": true,
				"original_type": "orig",
				"properties": {
					"k": "v"
				}
			},
			{
				"table_schema": "schema",
				"table_name": "table",
				"path": "path",
				"name": "b",
				"type": "data",
				"key": true,
				"fake_key": true,
				"required": true,
				"expression": "expression",
				"system_key": true,
				"original_type": "orig",
				"properties": {
					"x": "y"
				}
			}
		],
		"oldkeys": {
			"keynames": [
				"a",
				"b"
			],
			"keytypes": [
				"at",
				"bt"
			],
			"keyvalues": [
				"av",
				"bv"
			]
		},
		"tx_id": "tx_id",
		"query": "query"
	}`

	changeItemYSON = `{
		id=100u;
		nextlsn=200u;
		commitTime=1961u;
		txPosition=9;
		kind=insert;
		schema=schema;
		table=table;
		columnnames=[
			a;
			b;
		];
		columnvalues=[
			av;
			bv;
		];
		"table_schema"=[
			{
				TableSchema=schema;
				TableName=table;
				Path=path;
				ColumnName=a;
				DataType=data;
				PrimaryKey=%true;
				FakeKey=%true;
				Required=%true;
				Expression=expression;
				OriginalType=orig;
				Properties={
					k=v;
				};
			};
			{
				TableSchema=schema;
				TableName=table;
				Path=path;
				ColumnName=b;
				DataType=data;
				PrimaryKey=%true;
				FakeKey=%true;
				Required=%true;
				Expression=expression;
				OriginalType=orig;
				Properties={
					x=y;
				};
			};
		];
		oldkeys={
			KeyNames=[
				a;
				b;
			];
			KeyTypes=[
				at;
				bt;
			];
			KeyValues=[
				av;
				bv;
			];
		};
		tx_id=tx_id;
	}`
)

func TestCollapse(t *testing.T) {
	t.Run("insert and update primary key", func(t *testing.T) {
		tableSchema := NewTableSchema([]ColSchema{{ColumnName: "id", PrimaryKey: true}})
		changes := []ChangeItem{
			{
				Kind:         InsertKind,
				TableSchema:  tableSchema,
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{1},
			},
			{
				Kind:         UpdateKind,
				TableSchema:  tableSchema,
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{2},
				OldKeys: OldKeysType{
					KeyNames:  []string{"id"},
					KeyValues: []interface{}{1},
				},
			},
			{
				Kind:         UpdateKind,
				TableSchema:  tableSchema,
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{3},
				OldKeys: OldKeysType{
					KeyNames:  []string{"id"},
					KeyValues: []interface{}{2},
				},
			},
		}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		require.Equal(t, 1, len(res))
		require.Equal(t, InsertKind, res[0].Kind)
		require.Equal(t, []interface{}{3}, res[0].ColumnValues)
	})
	t.Run("insert and update primary key sequentially", func(t *testing.T) {
		tableSchema := NewTableSchema([]ColSchema{{ColumnName: "id", PrimaryKey: true}})
		changes := []ChangeItem{
			{
				Kind:         InsertKind,
				TableSchema:  tableSchema,
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{1},
			},
		}
		finalID := 10
		for i := 1; i < finalID; i++ {
			changes = append(changes, ChangeItem{
				Kind:         UpdateKind,
				TableSchema:  tableSchema,
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{i + 1},
				OldKeys:      OldKeysType{KeyNames: []string{"id"}, KeyValues: []interface{}{i}},
			})
		}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		require.Equal(t, 1, len(res))
		require.Equal(t, InsertKind, res[0].Kind)
		require.Equal(t, []interface{}{finalID}, res[0].ColumnValues)
	})
	t.Run("Update Update, Diff toast", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:          291971525,
			CommitTime:  1601382119000000000,
			Kind:        UpdateKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"FIO",
				"minus_words",
			},
			ColumnValues: []interface{}{
				51615524,
				"ООО \"РостовПромПокрытия\"",
				"[\"бесплатно\",\"борода\",\"игры\",\"порно\"]",
			},
		}, {
			ID:          291975574,
			CommitTime:  1601382119000000000,
			Kind:        UpdateKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615524,
				"[{\"goal_id\":\"114403594\",\"value\":500}]",
			},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		assert.Equal(t, len(res), 1)
		assert.Equal(t, res[0].Kind, UpdateKind)
		assert.Contains(t, res[0].ColumnValues, "[\"бесплатно\",\"борода\",\"игры\",\"порно\"]")
		assert.Contains(t, res[0].ColumnValues, "[{\"goal_id\":\"114403594\",\"value\":500}]")
		assert.Contains(t, res[0].ColumnNames, "minus_words")
	})
	t.Run("Update Update, Diff full", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:         291971525,
			CommitTime: 1601382119000000000,
			Kind:       UpdateKind,
			Schema:     "ppc",
			Table:      "camp_options",
			TableSchema: NewTableSchema([]ColSchema{
				{PrimaryKey: true, ColumnName: "cid"},
				{PrimaryKey: false, ColumnName: "FIO"},
				{PrimaryKey: false, ColumnName: "minus_words"},
			}),
			ColumnNames: []string{
				"cid",
				"FIO",
				"minus_words",
			},
			ColumnValues: []interface{}{
				51615524,
				"ООО \"РостовПромПокрытия\"",
				"[\"бесплатно\",\"борода\",\"игры\",\"порно\"]",
			},
			OldKeys: OldKeysType{
				KeyNames: []string{
					"cid",
					"FIO",
					"minus_words",
				},
				KeyValues: []interface{}{
					"51615524",
					"",
					"",
				},
			},
		}, {
			ID:         291975574,
			CommitTime: 1601382119000000000,
			Kind:       UpdateKind,
			Schema:     "ppc",
			Table:      "camp_options",
			TableSchema: NewTableSchema([]ColSchema{
				{PrimaryKey: true, ColumnName: "cid"},
				{PrimaryKey: false, ColumnName: "FIO"},
				{PrimaryKey: false, ColumnName: "minus_words"},
			}),
			ColumnNames: []string{
				"cid",
				"FIO",
				"minus_words",
			},
			ColumnValues: []interface{}{
				51615524,
				"ООО \"РостовПромПокрытия\"",
				"[\"бесплатно\",\"борода\",\"игры\",\"порно\",\"без смс\"]",
			},
			OldKeys: OldKeysType{
				KeyNames: []string{
					"cid",
					"FIO",
					"minus_words",
				},
				KeyValues: []interface{}{
					"51615524",
					"ООО \"РостовПромПокрытия\"",
					"[\"бесплатно\",\"борода\",\"игры\",\"порно\"]",
				},
			},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		assert.Equal(t, len(res), 1)
		assert.Equal(t, res[0].Kind, UpdateKind)
		assert.Contains(t, res[0].ColumnValues, "[\"бесплатно\",\"борода\",\"игры\",\"порно\",\"без смс\"]")
		assert.Contains(t, res[0].ColumnValues, "ООО \"РостовПромПокрытия\"")
		assert.Contains(t, res[0].ColumnValues, 51615524)
		assert.Contains(t, res[0].ColumnNames, "minus_words")
		assert.Contains(t, res[0].ColumnNames, "FIO")
		assert.Contains(t, res[0].ColumnNames, "cid")
	})
	t.Run("Insert Update, Diff Toast", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:          291971525,
			CommitTime:  1601382119000000000,
			Kind:        InsertKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"minus_words",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615524,
				"[\"бесплатно\",\"борода\",\"игры\",\"порно\"]",
				"[{\"value\":100}]",
			},
		}, {
			ID:          291975574,
			CommitTime:  1601382119000000000,
			Kind:        UpdateKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615524,
				"[{\"value\":500}]",
			},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		assert.Equal(t, len(res), 1)
		assert.Equal(t, res[0].Kind, InsertKind)
		assert.Contains(t, res[0].ColumnValues, "[\"бесплатно\",\"борода\",\"игры\",\"порно\"]")
		assert.Contains(t, res[0].ColumnValues, "[{\"value\":500}]")
		assert.NotContains(t, res[0].ColumnValues, "[{\"value\":100}]")
		assert.Contains(t, res[0].ColumnNames, "minus_words")
	})

	t.Run("Insert Update, multiple PK", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:           1,
			CommitTime:   1,
			Kind:         InsertKind,
			Schema:       "schema",
			Table:        "table",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "i2"}, {PrimaryKey: true, ColumnName: "i1"}, {PrimaryKey: true, ColumnName: "i3"}}),
			ColumnNames:  []string{"i1", "i2", "i3", "t"},
			ColumnValues: []interface{}{10 + 1, 20 + 1, 30 + 1, "test1"},
		}, {
			ID:           2,
			CommitTime:   2,
			Kind:         UpdateKind,
			Schema:       "schema",
			Table:        "table",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "i1"}, {PrimaryKey: true, ColumnName: "i3"}, {PrimaryKey: true, ColumnName: "i2"}}),
			ColumnNames:  []string{"i2", "i1", "i3", "t"},
			ColumnValues: []interface{}{20 + 1, 10 + 1, 30 + 1, "test2"},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		assert.Equal(t, len(res), 1)
		assert.Equal(t, res[0].Kind, InsertKind)
		assert.Contains(t, res[0].ColumnValues, 10+1)
		assert.Contains(t, res[0].ColumnValues, 20+1)
		assert.Contains(t, res[0].ColumnValues, 30+1)
		assert.Contains(t, res[0].ColumnValues, "test2")
	})

	t.Run("Insert Update, multiple PK, toast", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:           1,
			CommitTime:   1,
			Kind:         InsertKind,
			Schema:       "schema",
			Table:        "table",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "i2"}, {PrimaryKey: true, ColumnName: "i1"}}),
			ColumnNames:  []string{"i1", "i2", "t"},
			ColumnValues: []interface{}{10 + 1, 20 + 1, "test1"},
		}, {
			ID:           2,
			CommitTime:   2,
			Kind:         UpdateKind,
			Schema:       "schema",
			Table:        "table",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "i1"}, {PrimaryKey: true, ColumnName: "i2"}}),
			ColumnNames:  []string{"t"},
			ColumnValues: []interface{}{"test2"},
			OldKeys: OldKeysType{
				KeyNames:  []string{"i2", "i1"},
				KeyValues: []interface{}{20 + 1, 10 + 1},
			},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, res[0].Kind, InsertKind)
		assert.Contains(t, res[0].ColumnValues, 10+1)
		assert.Contains(t, res[0].ColumnValues, 20+1)
		assert.Contains(t, res[0].ColumnValues, "test2")
	})

	t.Run("Insert Update Delete", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:          291971525,
			CommitTime:  1601382119000000000,
			Kind:        InsertKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"minus_words",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615524,
				"[\"бесплатно\",\"борода\",\"игры\",\"порно\"]",
				"[{\"value\":100}]",
			},
		}, {
			ID:          291975574,
			CommitTime:  1601382119000000000,
			Kind:        UpdateKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615524,
				"[{\"value\":500}]",
			},
		}, {
			ID:          291975574,
			CommitTime:  1601382119000000000,
			Kind:        DeleteKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
			},
			ColumnValues: []interface{}{
				51615524,
			},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		assert.Equal(t, len(res), 1)
		assert.Equal(t, res[0].Kind, DeleteKind)
	})
	t.Run("Update primary key and Delete", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:          291975576,
			CommitTime:  1601382119000000000,
			Kind:        UpdateKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615525,
				"[{\"value\":500}]",
			},
			OldKeys: OldKeysType{
				KeyNames:  []string{"cid"},
				KeyValues: []interface{}{51615524},
			},
		}, {
			ID:          291975576,
			CommitTime:  1601382119000000000,
			Kind:        DeleteKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{},
			OldKeys: OldKeysType{
				KeyNames:  []string{"cid"},
				KeyValues: []interface{}{51615525},
			},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		assert.Equal(t, len(res), 1)
		assert.Equal(t, res[0].Kind, DeleteKind)
		assert.Equal(t, res[0].OldKeys.KeyValues[0], 51615524)
	})
	t.Run("Insert Update Delete Insert", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:          291971525,
			CommitTime:  1601382119000000000,
			Kind:        InsertKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"minus_words",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615524,
				"[\"бесплатно\",\"борода\",\"игры\",\"порно\"]",
				"[{\"value\":100}]",
			},
		}, {
			ID:          291975574,
			CommitTime:  1601382119000000000,
			Kind:        UpdateKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615524,
				"[{\"value\":500}]",
			},
		}, {
			ID:          291975574,
			CommitTime:  1601382119000000000,
			Kind:        DeleteKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
			},
			ColumnValues: []interface{}{
				51615524,
			},
		}, {
			ID:          291971525,
			CommitTime:  1601382119000000000,
			Kind:        InsertKind,
			Schema:      "ppc",
			Table:       "camp_options",
			TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames: []string{
				"cid",
				"minus_words",
				"meaningful_goals",
			},
			ColumnValues: []interface{}{
				51615524,
				"[\"бесплатно\",\"борода\",\"игры\",\"порно\"]",
				"[{\"value\":200}]",
			},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		assert.Equal(t, len(res), 1)
		assert.Equal(t, res[0].Kind, InsertKind)
		assert.Contains(t, res[0].ColumnValues, "[{\"value\":200}]")
		assert.Contains(t, res[0].ColumnNames, "minus_words")
	})
	t.Run("Primary key change", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:           291971525,
			CommitTime:   1601382119000000000,
			Kind:         InsertKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames:  []string{"cid", "minus_words", "meaningful_goals"},
			ColumnValues: []interface{}{51615524, `["бесплатно","борода","игры","порно"]`, `[{"value":100}]`},
		}, {
			ID:           291975574,
			CommitTime:   1601382119000000001,
			Kind:         UpdateKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames:  []string{"cid", "meaningful_goals"},
			ColumnValues: []interface{}{51615525, `[{"value":500}]`},
			OldKeys: OldKeysType{
				KeyNames:  []string{"cid"},
				KeyTypes:  []string{"integer"},
				KeyValues: []interface{}{51615524},
			},
		}, {
			ID:           291971526,
			CommitTime:   1601382119000000002,
			Kind:         InsertKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
			ColumnNames:  []string{"cid", "minus_words", "meaningful_goals"},
			ColumnValues: []interface{}{51615526, `["платно", "качественный пристойный контент"]`, `[{"value":200}]`},
		}}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)

		require.Equal(t, 2, len(res))

		require.EqualValues(t, InsertKind, res[0].Kind)
		require.EqualValues(t, 3, len(res[0].ColumnNames))
		require.EqualValues(t, 3, len(res[0].ColumnValues))

		sort.Sort(ByID(res))
		checkIfKeyContainsValue(t, res[0], "cid", 51615525)
		checkIfKeyContainsValue(t, res[0], "minus_words", `["бесплатно","борода","игры","порно"]`)
		checkIfKeyContainsValue(t, res[0], "meaningful_goals", `[{"value":500}]`)

		require.EqualValues(t, InsertKind, res[1].Kind)
		require.EqualValues(t, 3, len(res[1].ColumnNames))
		require.EqualValues(t, 3, len(res[1].ColumnValues))
		checkIfKeyContainsValue(t, res[1], "cid", 51615526)
		checkIfKeyContainsValue(t, res[1], "minus_words", `["платно", "качественный пристойный контент"]`)
		checkIfKeyContainsValue(t, res[1], "meaningful_goals", `[{"value":200}]`)
	})
	t.Run("Bad collapse with extra contstraint", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:           291971525,
			CommitTime:   1601382119000000000,
			Kind:         InsertKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}, {ColumnName: "uniq"}}),
			ColumnNames:  []string{"cid", "uniq"},
			ColumnValues: []interface{}{1, "first_value"},
		}, {
			ID:           291975574,
			CommitTime:   1601382119000000000,
			Kind:         InsertKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}, {ColumnName: "uniq"}}),
			ColumnNames:  []string{"cid", "uniq"},
			ColumnValues: []interface{}{2, "second_value"},
		}} // first TX init uniq values
		changes2 := []ChangeItem{{
			ID:           291975574,
			CommitTime:   1601382119000000000,
			Kind:         UpdateKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}, {ColumnName: "uniq"}}),
			ColumnNames:  []string{"cid", "uniq"},
			ColumnValues: []interface{}{1, "temp_value"},
		}, {
			ID:           291975574,
			CommitTime:   1601382119000000000,
			Kind:         UpdateKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}, {ColumnName: "uniq"}}),
			ColumnNames:  []string{"cid", "uniq"},
			ColumnValues: []interface{}{2, "first_value"},
		}, {
			ID:           291975574,
			CommitTime:   1601382119000000000,
			Kind:         UpdateKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}, {ColumnName: "uniq"}}),
			ColumnNames:  []string{"cid", "uniq"},
			ColumnValues: []interface{}{1, "second_value"},
		}} // swap uniq values between PKeys with intermediate temp value
		Dump(changes)
		Dump(changes2)
		res := Collapse(changes)
		res2 := Collapse(changes2)
		Dump(res)
		Dump(res2)
		assert.Equal(t, len(res), 2)
		assert.Equal(t, len(res2), 2)
		assert.Equal(t, res[0].Kind, InsertKind)
		assert.Equal(t, res2[0].Kind, UpdateKind)
	})
	t.Run("Collapse with no primary keys should be no-op", func(t *testing.T) {
		changes := []ChangeItem{{
			ID:           291971525,
			CommitTime:   1601382119000000000,
			Kind:         InsertKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: false, ColumnName: "cid"}, {ColumnName: "uniq"}}),
			ColumnNames:  []string{"cid", "uniq"},
			ColumnValues: []interface{}{1, "first_value"},
		}, {
			ID:           291975574,
			CommitTime:   1601382119000000000,
			Kind:         InsertKind,
			Schema:       "ppc",
			Table:        "camp_options",
			TableSchema:  NewTableSchema([]ColSchema{{PrimaryKey: false, ColumnName: "cid"}, {ColumnName: "uniq"}}),
			ColumnNames:  []string{"cid", "uniq"},
			ColumnValues: []interface{}{2, "second_value"},
		}} // first TX init uniq values
		res := Collapse(changes)
		require.EqualValues(t, changes, res)
	})
	t.Run("Collapse with mongo replication delete->insert records with same pkey", func(t *testing.T) {
		arrInStr := `
		[
		{
			"id": 0,
			"nextlsn": 1623427449,
			"commitTime": 1623427449000000000,
			"txPosition": 96,
			"kind": "delete",
			"schema": "startrek",
			"table": "issueLinkChains",
			"columnnames": null,
			"table_schema": [{
				"path": "",
				"name": "_id",
				"type": "string",
				"key": true,
				"required": false
			}, {
				"path": "",
				"name": "document",
				"type": "any",
				"key": false,
				"required": false
			}],
			"oldkeys": {
				"keynames": ["_id"],
				"keyvalues": [
					[
						{"Key": "issueId", "Value": "60be59389f7e4745883817d6"},
						{"Key": "linkField", "Value": "parentIssueLinkChain"}
					]
				]
			},
			"tx_id": "tx_id"
		}
		,
		{
			"id": 0,
			"nextlsn": 1623427449,
			"commitTime": 1623427449000000000,
			"txPosition": 105,
			"kind": "insert",
			"schema": "startrek",
			"table": "issueLinkChains",
			"columnnames": ["_id", "document"],
			"columnvalues": [
				[
					{"Key": "issueId", "Value": "60be59389f7e4745883817d6"},
					{"Key": "linkField", "Value": "parentIssueLinkChain"}
				],
				{
					"chain": [{
						"height": 1,
						"issue": "608999a8a702ca1877a3ddf2"
					}, {
						"height": 2,
						"issue": "6024fc6b5dc51554943bed7f"
					}, {
						"height": 3,
						"issue": "6017da0f3ed4d47bd0582d07"
					}]
				}
			],
			"table_schema": [{
				"path": "",
				"name": "_id",
				"type": "string",
				"key": true,
				"required": false
			}, {
				"path": "",
				"name": "document",
				"type": "any",
				"key": false,
				"required": false
			}],
			"oldkeys": {},
			"tx_id": "tx_id"
		}
		]
		`
		var arrIn []ChangeItem
		err := json.Unmarshal([]byte(arrInStr), &arrIn)
		require.NoError(t, err)

		arrOut := Collapse(arrIn)
		require.Equal(t, 1, len(arrOut))

		require.True(t, reflect.DeepEqual(arrIn[1], arrOut[0]))
	})
	t.Run("update insert", func(t *testing.T) {
		tableSchema := NewTableSchema([]ColSchema{{ColumnName: "id", PrimaryKey: true}})
		changes := []ChangeItem{
			{
				Kind:         UpdateKind,
				TableSchema:  tableSchema,
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{2},
				OldKeys: OldKeysType{
					KeyNames:  []string{"id"},
					KeyValues: []interface{}{1},
				},
			},
			{
				Kind:         InsertKind,
				TableSchema:  tableSchema,
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{1},
			},
		}
		Dump(changes)
		res := Collapse(changes)
		Dump(res)
		require.True(t, reflect.DeepEqual(changes, res))
	})
}

type ByID []ChangeItem

func (s ByID) Len() int           { return len(s) }
func (s ByID) Less(i, j int) bool { return s[i].ID < s[j].ID }
func (s ByID) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func TestPkeyChange(t *testing.T) {
	schema1 := NewTableSchema([]ColSchema{
		{
			ColumnName: "id",
			PrimaryKey: true,
		}, {
			ColumnName: "value",
			PrimaryKey: false,
		},
	})
	require.False(t, (&ChangeItem{
		Kind: "update",
		OldKeys: OldKeysType{
			KeyNames:  []string{"id", "value"},
			KeyValues: []interface{}{1, "kek"},
		},
		ColumnNames:  []string{"id", "value"},
		ColumnValues: []interface{}{1, "kek"},
		TableSchema:  schema1,
	}).KeysChanged())

	require.False(t, (&ChangeItem{
		Kind: "update",
		OldKeys: OldKeysType{
			KeyNames:  []string{"id", "value"},
			KeyValues: []interface{}{1, "kek"},
		},
		ColumnNames:  []string{"id", "value"},
		ColumnValues: []interface{}{1, "lel"},
		TableSchema:  schema1,
	}).KeysChanged())

	require.True(t, (&ChangeItem{
		Kind: "update",
		OldKeys: OldKeysType{
			KeyNames:  []string{"id", "value"},
			KeyValues: []interface{}{1, "kek"},
		},
		ColumnNames:  []string{"id", "value"},
		ColumnValues: []interface{}{2, "kek"},
		TableSchema:  schema1,
	}).KeysChanged())

	require.True(t, (&ChangeItem{
		Kind: "update",
		OldKeys: OldKeysType{
			KeyNames:  []string{"id", "value"},
			KeyValues: []interface{}{1, "kek"},
		},
		ColumnNames:  []string{"id", "value"},
		ColumnValues: []interface{}{2, "lel"},
		TableSchema:  schema1,
	}).KeysChanged())

	schema2 := NewTableSchema([]ColSchema{
		{
			ColumnName: "id1",
			PrimaryKey: true,
		}, {
			ColumnName: "value1",
			PrimaryKey: false,
		}, {
			ColumnName: "id2",
			PrimaryKey: true,
		}, {
			ColumnName: "value2",
			PrimaryKey: false,
		},
	})

	require.False(t, (&ChangeItem{
		Kind: "update",
		OldKeys: OldKeysType{
			KeyNames:  []string{"id1", "value1", "id2", "value2"},
			KeyValues: []interface{}{1, "lel", 100, "kek"},
		},
		ColumnNames:  []string{"id1", "value1", "id2", "value2"},
		ColumnValues: []interface{}{1, "olel", 100, "okek"},
		TableSchema:  schema2,
	}).KeysChanged())

	require.True(t, (&ChangeItem{
		Kind: "update",
		OldKeys: OldKeysType{
			KeyNames:  []string{"id1", "value1", "id2", "value2"},
			KeyValues: []interface{}{1, "kek", 100, "lel"},
		},
		ColumnNames:  []string{"id1", "value1", "id2", "value2"},
		ColumnValues: []interface{}{1, "lel", 200, "lel"},
		TableSchema:  schema2,
	}).KeysChanged())

	require.True(t, (&ChangeItem{
		Kind: "update",
		OldKeys: OldKeysType{
			KeyNames:  []string{"id1", "id2"},
			KeyValues: []interface{}{1, 100},
		},
		ColumnNames:  []string{"id1", "value1", "id2", "value2"},
		ColumnValues: []interface{}{1, "lel", 200, "lel"},
		TableSchema:  schema2,
	}).KeysChanged())

	// Tests ported from MySQL

	t.Run("one column - PrimaryKey, changed", func(t *testing.T) {
		tableSchema00 := NewTableSchema([]ColSchema{{ColumnName: "a", PrimaryKey: true}})

		changeItem00 := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a"},
			ColumnValues: []interface{}{1},
			OldKeys: OldKeysType{
				KeyNames:  []string{"a"},
				KeyTypes:  []string{"integer"},
				KeyValues: []interface{}{123},
			},
			TableSchema: tableSchema00,
		}
		require.True(t, changeItem00.KeysChanged())

		// check it will work fine - if #cols in TableSchema more than in changeItem
		newColumns := append(tableSchema00.Columns(), ColSchema{ColumnName: "X", PrimaryKey: false})
		changeItem00.TableSchema = NewTableSchema(newColumns)
		require.True(t, true, changeItem00.KeysChanged())
	})

	t.Run("one column - not a PrimaryKey, changed", func(t *testing.T) {
		tableSchema01 := NewTableSchema([]ColSchema{{ColumnName: "a", PrimaryKey: false}})
		changeItem01 := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a"},
			ColumnValues: []interface{}{1},
			TableSchema:  tableSchema01,
		}

		require.False(t, changeItem01.KeysChanged())

		// check it will work fine - if #cols in TableSchema more than in changeItem
		newColumns := append(tableSchema01.Columns(), ColSchema{ColumnName: "X", PrimaryKey: false})
		changeItem01.TableSchema = NewTableSchema(newColumns)
		require.False(t, changeItem01.KeysChanged())
	})

	t.Run("two columns: both PrimaryKey, one 1st changed", func(t *testing.T) {
		tableSchema02 := NewTableSchema([]ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		})
		changeItem02 := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyTypes:  []string{"integer", "integer"},
				KeyValues: []interface{}{1, 3},
			},
			TableSchema: tableSchema02,
		}

		require.True(t, changeItem02.KeysChanged())

		// check it will work fine - if #cols in TableSchema more than in changeItem
		newColumns := append(tableSchema02.Columns(), ColSchema{ColumnName: "X", PrimaryKey: false})
		changeItem02.TableSchema = NewTableSchema(newColumns)
		require.True(t, changeItem02.KeysChanged())
	})

	t.Run("two columns: both PrimaryKey, one 2nd changed", func(t *testing.T) {
		tableSchema04 := NewTableSchema([]ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		})
		changeItem04 := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyTypes:  []string{"integer", "integer"},
				KeyValues: []interface{}{2, 2},
			},
			TableSchema: tableSchema04,
		}

		require.True(t, changeItem04.KeysChanged())

		// check it will work fine - if #cols in TableSchema more than in changeItem
		newColumns := append(tableSchema04.Columns(), ColSchema{ColumnName: "X", PrimaryKey: false})
		changeItem04.TableSchema = NewTableSchema(newColumns)
		require.True(t, changeItem04.KeysChanged())
	})

	t.Run("two columns: both PrimaryKey, both changed", func(t *testing.T) {
		tableSchema05 := NewTableSchema([]ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		})
		changeItem05 := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyTypes:  []string{"integer", "integer"},
				KeyValues: []interface{}{3, 4},
			},
			TableSchema: tableSchema05,
		}

		require.True(t, changeItem05.KeysChanged())

		// check it will work fine - if #cols in TableSchema more than in changeItem
		newColumns := append(tableSchema05.Columns(), ColSchema{ColumnName: "X", PrimaryKey: false})
		changeItem05.TableSchema = NewTableSchema(newColumns)
		require.True(t, changeItem05.KeysChanged())
	})

	t.Run("two columns: both PrimaryKey, both changed", func(t *testing.T) {
		tableSchema06 := NewTableSchema([]ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		})
		changeItem06 := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyTypes:  []string{"integer", "integer"},
				KeyValues: []interface{}{1, 2},
			},
			TableSchema: tableSchema06,
		}

		require.False(t, changeItem06.KeysChanged())

		// check it will work fine - if #cols in TableSchema more than in changeItem
		newColumns := append(tableSchema06.Columns(), ColSchema{ColumnName: "X", PrimaryKey: false})
		changeItem06.TableSchema = NewTableSchema(newColumns)
		require.False(t, changeItem06.KeysChanged())
	})

	t.Run("3 columns: 2 PrimaryKey + 1 not a PrimaryKey, not a PrimaryKey changed", func(t *testing.T) {
		tableSchema07 := NewTableSchema([]ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
			{ColumnName: "c", PrimaryKey: false},
		})
		changeItem07 := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a", "b", "c"},
			ColumnValues: []interface{}{1, 2, 3},
			OldKeys: OldKeysType{
				KeyNames:  []string{"a", "b", "c"},
				KeyTypes:  []string{"integer", "integer", "integer"},
				KeyValues: []interface{}{1, 2, 4},
			},
			TableSchema: tableSchema07,
		}

		require.False(t, changeItem07.KeysChanged())

		// check it will work fine - if #cols in TableSchema more than in changeItem
		newColumns := append(tableSchema07.Columns(), ColSchema{ColumnName: "X", PrimaryKey: false})
		changeItem07.TableSchema = NewTableSchema(newColumns)
		require.False(t, changeItem07.KeysChanged())
	})
}

func TestSplitByID(t *testing.T) {
	t.Run("multiple", func(t *testing.T) {
		txs := SplitByID([]ChangeItem{
			{ID: 1},
			{ID: 1},
			{ID: 1},
			{ID: 1},
			{ID: 2},
			{ID: 2},
			{ID: 2},
			{ID: 3},
		})
		require.Equal(t, 3, len(txs))
		require.Equal(t, []TxBound{{
			Left:  0,
			Right: 4,
		}, {
			Left:  4,
			Right: 7,
		}, {
			Left:  7,
			Right: 8,
		}}, txs)
	})
	t.Run("single", func(t *testing.T) {
		txs := SplitByID([]ChangeItem{
			{ID: 1}, {ID: 1}, {ID: 1}, {ID: 1},
		})
		require.Equal(t, 1, len(txs))
		require.Equal(t, []TxBound{{
			Left:  0,
			Right: 4,
		}}, txs)
	})
	t.Run("empty", func(t *testing.T) {
		txs := SplitByID([]ChangeItem{})
		require.Equal(t, 0, len(txs))
	})
}

// TestSplitUnchangedKeys tell us a story about John and Ben:
// This story was told us by a guy named chat-gpt.
//
//	Hark! In fair Verona, there once was John
//	Who settled down upon 123 street, anon.
//	Alas, his heart yearned for a change, a shift,
//	And he moved across the street to 124 street, swift.
//
//	There, he underwent a transformation grand,
//	Declaring himself as her, fair Susan, so grand.
//	Meanwhile, on 100 street, dwelled Ben,
//	Who fatefully crossed paths with Susan, then.
//
//	Their hearts entwined, a love story unfurled,
//	Living together on 124 street, a new world.
//	Thus, in this tale of twists and turns, we find,
//	A life transformed, two souls now intertwined.
func TestSplitUnchangedKeys(t *testing.T) {
	schema := NewTableSchema([]ColSchema{
		{ColumnName: "name", PrimaryKey: true},
		{ColumnName: "address"},
	})
	cols := []string{"name", "address"}

	changes := []ChangeItem{
		{
			Kind:         "insert",
			ColumnNames:  cols,
			ColumnValues: []interface{}{"John", "123 Street"},
			TableSchema:  schema,
		},
		{
			Kind:        "update",
			ColumnNames: cols,
			OldKeys: OldKeysType{
				KeyNames:  cols,
				KeyValues: []interface{}{"John", "123 Street"},
			},
			ColumnValues: []interface{}{"John", "124 Street"},
			TableSchema:  schema,
		},
		{
			Kind:        "update",
			ColumnNames: cols,
			OldKeys: OldKeysType{
				KeyNames:  cols,
				KeyValues: []interface{}{"John", "124 Street"}},
			ColumnValues: []interface{}{"Susan", "124 Street"},
			TableSchema:  schema,
		},
		{
			Kind:         "insert",
			ColumnNames:  cols,
			ColumnValues: []interface{}{"Ben", "100 Street"},
			TableSchema:  schema,
		},
		{
			Kind:        "update",
			ColumnNames: cols,
			OldKeys: OldKeysType{
				KeyNames:  cols,
				KeyValues: []interface{}{"Ben", "100 Street"},
			},
			ColumnValues: []interface{}{"Ben", "124 Street"},
			TableSchema:  schema,
		},
	}

	result := SplitUpdatedPKeys(changes)

	expected := [][]ChangeItem{
		{
			{
				Kind:         "insert",
				ColumnNames:  cols,
				ColumnValues: []interface{}{"John", "123 Street"},
				TableSchema:  schema,
			},
			{
				Kind:        "update",
				ColumnNames: cols,
				OldKeys: OldKeysType{
					KeyNames:  cols,
					KeyValues: []interface{}{"John", "123 Street"},
				},
				ColumnValues: []interface{}{"John", "124 Street"},
				TableSchema:  schema,
			},
		},
		{
			{
				Kind:        "delete",
				ColumnNames: nil,
				Counter:     0,
				OldKeys: OldKeysType{
					KeyNames:  cols,
					KeyValues: []interface{}{"John", "124 Street"}},
				ColumnValues: nil,
				TableSchema:  schema,
			},
			{
				Kind:         "insert",
				ColumnNames:  cols,
				Counter:      1,
				OldKeys:      OldKeysType{},
				ColumnValues: []interface{}{"Susan", "124 Street"},
				TableSchema:  schema,
			},
		},
		{
			{
				Kind:         "insert",
				ColumnNames:  cols,
				ColumnValues: []interface{}{"Ben", "100 Street"},
				TableSchema:  schema,
			},
			{
				Kind:        "update",
				ColumnNames: cols,
				OldKeys: OldKeysType{
					KeyNames:  cols,
					KeyValues: []interface{}{"Ben", "100 Street"},
				},
				ColumnValues: []interface{}{"Ben", "124 Street"},
				TableSchema:  schema,
			},
		},
	}
	require.Equal(t, expected, result)
}

func checkIfKeyContainsValue(t *testing.T, changeItem ChangeItem, key string, val interface{}) {
	for i := range changeItem.ColumnNames {
		if changeItem.ColumnNames[i] == key {
			require.EqualValues(t, changeItem.ColumnValues[i], val)
		}
	}
	require.Error(t, xerrors.Errorf("changeItem doesn't contain key %s", key))
}

func TestNewPartition(t *testing.T) {
	require.Equal(t, "{\"cluster\":\"vla\",\"partition\":1,\"topic\":\"yabs-rt/bs-tracking-log\"}", NewPartition("rt3.vla--yabs-rt--bs-tracking-log", 1).String())
	require.Equal(t, "{\"cluster\":\"kafka-bs\",\"partition\":2,\"topic\":\"cdc/prod/edadeal_wallet\"}", NewPartition("rt3.kafka-bs--cdc@prod--edadeal_wallet", 2).String())
}

type canondata struct {
	Pointer string
	Value   string
}

func TestMarshalYSON(t *testing.T) {
	marshalYSON := func(value any) string {
		var buf bytes.Buffer
		writer := yson.NewWriterConfig(&buf, yson.WriterConfig{Format: yson.FormatPretty})
		encoder := yson.NewEncoderWriter(writer)
		require.NoError(t, encoder.Encode(&value))
		return buf.String()
	}

	canon.SaveJSON(t, canondata{
		Pointer: marshalYSON(&testChangeItem),
		Value:   marshalYSON(testChangeItem),
	})
}

func TestMarshalJSON(t *testing.T) {
	marshalJSON := func(value any) string {
		jsonData, err := json.MarshalIndent(value, "", "    ")
		require.NoError(t, err)
		return string(jsonData)
	}
	canon.SaveJSON(t, canondata{
		Pointer: marshalJSON(&testChangeItem),
		Value:   marshalJSON(testChangeItem),
	})
}

func TestUnmarshalYSON(t *testing.T) {
	var unmarshalled ChangeItem
	require.NoError(t, yson.Unmarshal([]byte(changeItemYSON), &unmarshalled))
	unmarshalled.PartID = "part_id"
	unmarshalled.Query = "query"
	isEqual := reflect.DeepEqual(unmarshalled, testChangeItem)
	require.True(t, isEqual, "%v != %v", testChangeItem, unmarshalled)
}

func TestUnmarshalJSON(t *testing.T) {
	var unmarshalled ChangeItem
	require.NoError(t, json.Unmarshal([]byte(changeItemJSON), &unmarshalled))
	isEqual := reflect.DeepEqual(unmarshalled, testChangeItem)
	require.True(t, isEqual, "%v != %v", testChangeItem, unmarshalled)
}

func TestRemoveColumns(t *testing.T) {
	t.Run("insert change item", func(t *testing.T) {
		schema := NewTableSchema([]ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		})
		ci := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys:      OldKeysType{},
			TableSchema:  schema,
		}
		ci.RemoveColumns("a")
		require.Equal(t, []string{"b"}, ci.ColumnNames)
		require.Equal(t, []interface{}{2}, ci.ColumnValues)
	})
	t.Run("update change item", func(t *testing.T) {
		schema := NewTableSchema([]ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		})
		ci := ChangeItem{
			Kind:         "update",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyTypes:  []string{"string", "integer"},
				KeyValues: []interface{}{1, 2},
			},
			TableSchema: schema,
		}
		ci.RemoveColumns("a")
		require.Equal(t, []string{"b"}, ci.ColumnNames)
		require.Equal(t, []interface{}{2}, ci.ColumnValues)
		require.Equal(t, []string{"b"}, ci.OldKeys.KeyNames)
		require.Equal(t, []interface{}{2}, ci.OldKeys.KeyValues)
		require.Equal(t, []string{"integer"}, ci.OldKeys.KeyTypes)
	})
}

func TestApplyMask(t *testing.T) {
	tests := []struct {
		name   string
		input  []string
		mask   []byte
		output []string
	}{
		{
			name:   "No deletion",
			input:  []string{"a", "b", "c", "d", "e"},
			mask:   []byte{1, 1, 1, 1, 1},
			output: []string{"a", "b", "c", "d", "e"},
		},
		{
			name:   "Delete single element",
			input:  []string{"a", "b", "c", "d", "e"},
			mask:   []byte{1, 1, 0, 1, 1, 1},
			output: []string{"a", "b", "d", "e"},
		},
		{
			name:   "Delete multiple elements",
			input:  []string{"a", "b", "c", "d", "e"},
			mask:   []byte{0, 1, 0, 1, 0},
			output: []string{"b", "d"},
		},
		{
			name:   "Delete all elements",
			input:  []string{"a", "b", "c", "d", "e"},
			mask:   []byte{0, 0, 0, 0, 0},
			output: []string{},
		},
		{
			name:   "Delete last element",
			input:  []string{"a", "b", "c", "d", "e"},
			mask:   []byte{1, 1, 1, 1, 0},
			output: []string{"a", "b", "c", "d"},
		},
		{
			name:   "Delete first element",
			input:  []string{"a", "b", "c", "d", "e"},
			mask:   []byte{0, 1, 1, 1, 1},
			output: []string{"b", "c", "d", "e"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := filterMask(test.input, test.mask)
			require.Equal(t, test.output, result)
		})
	}
}
