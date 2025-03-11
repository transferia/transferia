package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestRawColumnSerializerEmptyInput(t *testing.T) {
	rawColumnSerializer := NewRawColumnSerializer("test_column", logger.Log)

	batches, err := rawColumnSerializer.Serialize([]abstract.ChangeItem{})
	require.NoError(t, err)
	require.Len(t, batches, 0)
}

func TestRawColumnSerializerString(t *testing.T) {
	rawColumnSerializer := NewRawColumnSerializer("test_column", logger.Log)

	batches, err := rawColumnSerializer.Serialize([]abstract.ChangeItem{
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"extra_column", "test_column", "other_extra_column"},
			ColumnValues: []any{int64(1), "kek", nil},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"extra_column", "other_extra_column", "test_column"},
			ColumnValues: []any{int64(2), nil, "lel"},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"test_column", "other_extra_column", "extra_column"},
			ColumnValues: []any{"wtf", nil, int64(3)},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
	})
	require.NoError(t, err)
	require.Len(t, batches, 1)

	tablePartID := abstract.TablePartID{
		TableID: abstract.TableID{
			Namespace: "public",
			Name:      "test_table",
		},
		PartID: "p1",
	}
	require.Equal(t, []SerializedMessage{
		{Key: nil, Value: []byte("kek")},
		{Key: nil, Value: []byte("lel")},
		{Key: nil, Value: []byte("wtf")},
	}, batches[tablePartID])
}

func TestRawColumnSerializerMissingColumn(t *testing.T) {
	rawColumnSerializer := NewRawColumnSerializer("test_column", logger.Log)

	batches, err := rawColumnSerializer.Serialize([]abstract.ChangeItem{
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"extra_column", "other_extra_column"},
			ColumnValues: []any{int64(1), nil},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"extra_column", "other_extra_column", "test_column"},
			ColumnValues: []any{int64(2), nil, []byte("lel")},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"test_column", "other_extra_column", "extra_column"},
			ColumnValues: []any{[]byte("wtf"), nil, int64(3)},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
	})
	require.NoError(t, err)
	require.Len(t, batches, 1)

	tablePartID := abstract.TablePartID{
		TableID: abstract.TableID{
			Namespace: "public",
			Name:      "test_table",
		},
		PartID: "p1",
	}
	require.Equal(t, []SerializedMessage{
		{Key: nil, Value: []byte("lel")},
		{Key: nil, Value: []byte("wtf")},
	}, batches[tablePartID])
}

func TestRawColumnSerializerUnexpectedType(t *testing.T) {
	rawColumnSerializer := NewRawColumnSerializer("test_column", logger.Log)

	batches, err := rawColumnSerializer.Serialize([]abstract.ChangeItem{
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"extra_column", "table_column", "other_extra_column"},
			ColumnValues: []any{int64(1), 100500, nil},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"extra_column", "other_extra_column", "test_column"},
			ColumnValues: []any{int64(2), nil, int64(100500)},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"test_column", "other_extra_column", "extra_column"},
			ColumnValues: []any{"wtf", nil, int64(3)},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
	})
	require.NoError(t, err)
	require.Len(t, batches, 1)

	tablePartID := abstract.TablePartID{
		TableID: abstract.TableID{
			Namespace: "public",
			Name:      "test_table",
		},
		PartID: "p1",
	}
	require.Equal(t, []SerializedMessage{
		{Key: nil, Value: []byte("wtf")},
	}, batches[tablePartID])
}

func TestRawColumnSerializerInvalidTableSchema(t *testing.T) {
	rawColumnSerializer := NewRawColumnSerializer("test_column", logger.Log)

	batches, err := rawColumnSerializer.Serialize([]abstract.ChangeItem{
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"extra_column", "table_column", "other_extra_column"},
			ColumnValues: []any{int64(1), "kek", nil},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"extra_column", "other_extra_column", "test_column"},
			ColumnValues: []any{int64(2), nil, "lel"},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
		{
			PartID:       "p1",
			Schema:       "public",
			Table:        "test_table",
			ColumnNames:  []string{"test_column", "other_extra_column", "extra_column"},
			ColumnValues: []any{"wtf", nil, int64(3)},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "extra_column", DataType: string(schema.TypeInt64)},
				{ColumnName: "test_column", DataType: string(schema.TypeString)},
				{ColumnName: "other_extra_column", DataType: string(schema.TypeAny)},
			}),
		},
	})
	require.NoError(t, err)
	require.Len(t, batches, 1)

	tablePartID := abstract.TablePartID{
		TableID: abstract.TableID{
			Namespace: "public",
			Name:      "test_table",
		},
		PartID: "p1",
	}
	require.Equal(t, []SerializedMessage{
		{Key: nil, Value: []byte("lel")},
		{Key: nil, Value: []byte("wtf")},
	}, batches[tablePartID])
}
