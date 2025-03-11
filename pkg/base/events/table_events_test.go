package events

import (
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/adapter"
	"github.com/transferia/transferia/pkg/base/types"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

func randomValue(col base.Column) (base.Value, error) {
	switch col.Type().(type) {
	case *types.StringType:
		v := gofakeit.Adverb()
		return types.NewDefaultStringValue(&v, col), nil
	case *types.Int8Type:
		v := gofakeit.Int8()
		return types.NewDefaultInt8Value(&v, col), nil
	case *types.Int16Type:
		v := gofakeit.Int16()
		return types.NewDefaultInt16Value(&v, col), nil
	case *types.Int32Type:
		v := gofakeit.Int32()
		return types.NewDefaultInt32Value(&v, col), nil
	case *types.Int64Type:
		v := gofakeit.Int64()
		return types.NewDefaultInt64Value(&v, col), nil
	case *types.DateTimeType:
		v := gofakeit.DateRange(time.Now().Add(-time.Hour*24), time.Now().Add(time.Hour*24))
		return types.NewDefaultDateTimeValue(&v, col), nil
	}
	return nil, xerrors.Errorf("%T not supported", col.Type())
}

func TestNewTableBuilder(t *testing.T) {
	table := adapter.NewTableFromLegacy(abstract.NewTableSchema([]abstract.ColSchema{
		{TableSchema: "foo", TableName: "bar", ColumnName: "id", DataType: string(yt_schema.TypeString), PrimaryKey: true},
		{TableSchema: "foo", TableName: "bar", ColumnName: "vl", DataType: string(yt_schema.TypeString), PrimaryKey: false},
		{TableSchema: "foo", TableName: "bar", ColumnName: "it", DataType: string(yt_schema.TypeInt32), PrimaryKey: false},
		{TableSchema: "foo", TableName: "bar", ColumnName: "ts", DataType: string(yt_schema.TypeDatetime), PrimaryKey: false},
	}), abstract.TableID{
		Namespace: "foo",
		Name:      "bar",
	})
	tBuilder := NewTableEventsBuilder(table)
	for i := 0; i < 100; i++ {
		rb := tBuilder.AddRow()
		for j := 0; j < table.ColumnsCount(); j++ {
			col := table.Column(j)
			val, err := randomValue(col)
			require.NoError(t, err)
			rb.AddValue(val)
		}
	}
	batch := tBuilder.AsBatch()
	var changes []abstract.ChangeItem
	for batch.Next() {
		ev, err := batch.Event()
		require.NoError(t, err)
		iev, ok := ev.(InsertEvent)
		require.True(t, ok)
		change, err := iev.ToOldChangeItem()
		require.NoError(t, err)
		changes = append(changes, *change)
	}
	abstract.Dump(changes)
}

func TestNewTableBuilder_YsonSerialization(t *testing.T) {
	table := adapter.NewTableFromLegacy(abstract.NewTableSchema([]abstract.ColSchema{
		{TableSchema: "foo", TableName: "bar", ColumnName: "id", DataType: string(yt_schema.TypeString), PrimaryKey: true},
		{TableSchema: "foo", TableName: "bar", ColumnName: "vl", DataType: string(yt_schema.TypeString), PrimaryKey: false},
		{TableSchema: "foo", TableName: "bar", ColumnName: "it", DataType: string(yt_schema.TypeInt32), PrimaryKey: false},
	}), abstract.TableID{
		Namespace: "foo",
		Name:      "bar",
	})
	tBuilder := NewTableEventsBuilder(table)
	for i := 0; i < 10; i++ {
		rb := tBuilder.AddRow()
		keyStr := fmt.Sprintf("key-%v", i)
		valStr := fmt.Sprintf("val-%v", i)
		pI := int32(i)
		rb.AddValue(types.NewDefaultStringValue(&keyStr, table.Column(0)))
		rb.AddValue(types.NewDefaultStringValue(&valStr, table.Column(1)))
		rb.AddValue(types.NewDefaultInt32Value(&pI, table.Column(2)))
	}
	batch := tBuilder.AsBatch()
	var changes []abstract.ChangeItem
	for batch.Next() {
		ev, err := batch.Event()
		require.NoError(t, err)
		iev, ok := ev.(InsertEvent)
		require.True(t, ok)
		change, err := iev.ToOldChangeItem()
		require.NoError(t, err)
		changes = append(changes, *change)
	}
	abstract.Dump(changes)
	d, err := yson.Marshal(tBuilder.AsBatch())
	require.NoError(t, err)
	var rows []interface{}
	for i := 0; i < 10; i++ {
		row := map[string]interface{}{}
		keyStr := fmt.Sprintf("key-%v", i)
		valStr := fmt.Sprintf("val-%v", i)
		pI := int32(i)
		row["id"] = &keyStr
		row["vl"] = &valStr
		row["it"] = &pI
		rows = append(rows, row)
	}
	d2, err := yson.Marshal(rows)
	require.NoError(t, err)
	var rows1 interface{}
	var rows2 interface{}
	require.NoError(t, yson.Unmarshal(d, &rows1))
	require.NoError(t, yson.Unmarshal(d2, &rows2))
	require.Len(t, rows1, 10)
	require.Len(t, rows2, 10)
	require.Equal(t, rows1, rows2)
}

func BenchmarkEventTableToYson(b *testing.B) {
	b.Run("with yson optimization", func(b *testing.B) {
		table := adapter.NewTableFromLegacy(abstract.NewTableSchema([]abstract.ColSchema{
			{TableSchema: "foo", TableName: "bar", ColumnName: "id", DataType: string(yt_schema.TypeString), PrimaryKey: true},
			{TableSchema: "foo", TableName: "bar", ColumnName: "vl", DataType: string(yt_schema.TypeString), PrimaryKey: false},
			{TableSchema: "foo", TableName: "bar", ColumnName: "it", DataType: string(yt_schema.TypeInt32), PrimaryKey: false},
		}), abstract.TableID{
			Namespace: "foo",
			Name:      "bar",
		})
		tBuilder := NewTableEventsBuilder(table)
		for i := 0; i < b.N; i++ {
			rb := tBuilder.AddRow()
			keyStr := fmt.Sprintf("key-%v", i)
			valStr := fmt.Sprintf("val-%v", i)
			pI := int32(i)
			rb.AddValue(types.NewDefaultStringValue(&keyStr, table.Column(0)))
			rb.AddValue(types.NewDefaultStringValue(&valStr, table.Column(1)))
			rb.AddValue(types.NewDefaultInt32Value(&pI, table.Column(2)))
		}
		batch := tBuilder.AsBatch()
		d, err := yson.Marshal(batch)
		require.NoError(b, err)
		b.SetBytes(int64(len(d)))
		b.ReportAllocs()
	})

	b.Run("no yson optimization", func(b *testing.B) {
		var rows []interface{}
		for i := 0; i < b.N; i++ {
			row := map[string]interface{}{}
			keyStr := fmt.Sprintf("key-%v", i)
			valStr := fmt.Sprintf("val-%v", i)
			pI := int32(i)
			row["id"] = &keyStr
			row["vl"] = &valStr
			row["it"] = &pI
			rows = append(rows, row)
		}
		d, err := yson.Marshal(rows)
		require.NoError(b, err)
		b.SetBytes(int64(len(d)))
		b.ReportAllocs()
	})
}
