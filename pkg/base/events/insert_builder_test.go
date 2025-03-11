package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/base/adapter"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

func TestInsertBuilder_Build(t *testing.T) {
	sampleTbl := adapter.NewTableFromLegacy(abstract.NewTableSchema([]abstract.ColSchema{
		{TableSchema: "foo", TableName: "bar", ColumnName: "id", DataType: string(yt_schema.TypeString), PrimaryKey: true},
		{TableSchema: "foo", TableName: "bar", ColumnName: "vl", DataType: string(yt_schema.TypeString), Required: true, PrimaryKey: false},
		{TableSchema: "foo", TableName: "bar", ColumnName: "it", DataType: string(yt_schema.TypeInt32), PrimaryKey: false},
		{TableSchema: "foo", TableName: "bar", ColumnName: "ts", DataType: string(yt_schema.TypeDatetime), PrimaryKey: false},
	}), abstract.TableID{
		Namespace: "foo",
		Name:      "bar",
	})
	t.Run("basic from map", func(t *testing.T) {
		ev, err := NewDefaultInsertBuilder(sampleTbl).
			FromMap(map[string]interface{}{
				"id": "test_id",
				"vl": "test_value",
				"it": int32(123),
				"ts": time.Now().Add(time.Hour),
			}).
			Build()
		require.NoError(t, err)
		ci, err := ev.ToOldChangeItem()
		require.NoError(t, err)
		abstract.Dump([]abstract.ChangeItem{*ci})
	})
	t.Run("basic from columns", func(t *testing.T) {
		ev, err := NewDefaultInsertBuilder(sampleTbl).
			Column("id", "test_id").
			Column("vl", "test_value").
			Column("it", int32(123)).
			Column("ts", time.Now().Add(time.Hour)).
			Build()
		require.NoError(t, err)
		ci, err := ev.ToOldChangeItem()
		require.NoError(t, err)
		abstract.Dump([]abstract.ChangeItem{*ci})
	})
	t.Run("type mismatch from columns", func(t *testing.T) {
		_, err := NewDefaultInsertBuilder(sampleTbl).
			Column("id", "test_id").
			Column("vl", "test_value").
			Column("it", "123").
			Column("ts", time.Now().Add(time.Hour)).
			Build()
		require.Error(t, err)
	})
	t.Run("type mismatch from map", func(t *testing.T) {
		_, err := NewDefaultInsertBuilder(sampleTbl).
			FromMap(map[string]interface{}{
				"id": "test_id",
				"vl": "test_value",
				"it": "123",
				"ts": time.Now().Add(time.Hour),
			}).
			Build()
		require.Error(t, err)
	})
	t.Run("required missed from map", func(t *testing.T) {
		_, err := NewDefaultInsertBuilder(sampleTbl).
			FromMap(map[string]interface{}{
				"id": "test_id",
				"ts": time.Now().Add(time.Hour),
			}).
			Build()
		require.Error(t, err)
	})
	t.Run("optional missed from map", func(t *testing.T) {
		ev, err := NewDefaultInsertBuilder(sampleTbl).
			FromMap(map[string]interface{}{
				"id": "test_id",
				"vl": "test_value",
				"ts": time.Now().Add(time.Hour),
			}).
			Build()
		require.NoError(t, err)
		ci, err := ev.ToOldChangeItem()
		require.NoError(t, err)
		abstract.Dump([]abstract.ChangeItem{*ci})
	})
}
