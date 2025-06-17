//go:build !disable_yt_provider

package sink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestInferCommonType(t *testing.T) {
	common, compatible := inferCommonPrimitiveType(schema.TypeInt8, schema.TypeInt32)
	require.NoError(t, compatible)
	require.Equal(t, common, schema.TypeInt32)

	_, compatible = inferCommonPrimitiveType(schema.TypeInt8, schema.TypeUint32)
	require.Error(t, compatible)

	common, compatible = inferCommonPrimitiveType(schema.TypeInt8, schema.TypeAny)
	require.NoError(t, compatible)
	require.Equal(t, common, schema.TypeAny)

	common, compatible = inferCommonPrimitiveType(schema.TypeUint8, schema.TypeUint64)
	require.NoError(t, compatible)
	require.Equal(t, common, schema.TypeUint64)

	commonComplex, compatible := inferCommonComplexType(schema.TypeInt64, schema.Optional{Item: schema.TypeInt32})
	require.NoError(t, compatible)
	require.Equal(t, commonComplex, schema.Optional{Item: schema.TypeInt64})
}

func TestRequireness(t *testing.T) {
	require.True(t, inferCommonRequireness(true, true))
	require.False(t, inferCommonRequireness(false, false))
	require.False(t, inferCommonRequireness(false, true))
}

func TestTypeInferring(t *testing.T) {
	actual := schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:        "key",
				ComplexType: schema.TypeInt64,
				SortOrder:   schema.SortAscending,
			},
			{
				Name:        "value",
				ComplexType: schema.Optional{Item: schema.TypeInt32},
			},
		},
	}

	var united schema.Schema
	var err error

	t.Run("Test no changes", func(t *testing.T) {
		noChanges := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeInt32},
				},
			},
		}

		united, err = unionSchemas(actual, noChanges)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 2)

		require.Equal(t, united.Columns[1].Name, "value")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeInt32})
	})

	t.Run("Test type extension", func(t *testing.T) {
		typeExtension := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeInt64},
				},
			},
		}

		united, err = unionSchemas(actual, typeExtension)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 2)

		require.Equal(t, united.Columns[1].Name, "value")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeInt64})
	})

	t.Run("Test type reduction", func(t *testing.T) {
		typeReduction := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeInt16},
				},
			},
		}

		united, err = unionSchemas(actual, typeReduction)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 2)

		require.Equal(t, united.Columns[1].Name, "value")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeInt32})
	})
}

func TestUnionSchemas(t *testing.T) {
	actual := schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:        "key",
				ComplexType: schema.TypeInt64,
				SortOrder:   schema.SortAscending,
			},
			{
				Name:        "value",
				ComplexType: schema.TypeString,
			},
			{
				Name:        "extra",
				ComplexType: schema.Optional{Item: schema.TypeString},
			},
		},
	}
	var united schema.Schema
	var err error

	t.Run("Test change type and requireness", func(t *testing.T) {
		expected := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeBytes},
				},
				{
					Name:        "extra",
					ComplexType: schema.Optional{Item: schema.TypeString},
				},
			},
		}

		united, err = unionSchemas(actual, expected)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 3)

		require.Equal(t, united.Columns[0].Name, "key")
		require.Equal(t, united.Columns[0].SortOrder, schema.SortAscending)
		require.Equal(t, united.Columns[0].ComplexType, schema.TypeInt64)

		require.Equal(t, united.Columns[1].Name, "value")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeBytes})

		require.Equal(t, united.Columns[2].Name, "extra")
		require.Equal(t, united.Columns[2].ComplexType, schema.Optional{Item: schema.TypeString})
	})

	t.Run("Test reduction type", func(t *testing.T) {
		changed := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt32,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeBytes},
				},
				{
					Name:        "extra",
					ComplexType: schema.Optional{Item: schema.TypeString},
				},
			},
		}

		united, err = unionSchemas(actual, changed)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 3)

		require.Equal(t, united.Columns[0].Name, "key")
		require.Equal(t, united.Columns[0].SortOrder, schema.SortAscending)
		require.Equal(t, united.Columns[0].ComplexType, schema.TypeInt64)

		require.Equal(t, united.Columns[1].Name, "value")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeBytes})

		require.Equal(t, united.Columns[2].Name, "extra")
		require.Equal(t, united.Columns[2].ComplexType, schema.Optional{Item: schema.TypeString})
	})

	t.Run("Test add required column", func(t *testing.T) {
		expected1 := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeBytes},
				},
				{
					Name:        "extra",
					ComplexType: schema.Optional{Item: schema.TypeString},
				},
				{
					Name:        "extra1",
					ComplexType: schema.TypeString,
				},
			},
		}

		united, err = unionSchemas(united, expected1)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 4)

		require.Equal(t, united.Columns[0].Name, "key")
		require.Equal(t, united.Columns[0].SortOrder, schema.SortAscending)
		require.Equal(t, united.Columns[0].ComplexType, schema.TypeInt64)

		require.Equal(t, united.Columns[1].Name, "value")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeBytes})

		require.Equal(t, united.Columns[2].Name, "extra")
		require.Equal(t, united.Columns[2].ComplexType, schema.Optional{Item: schema.TypeString})

		require.Equal(t, united.Columns[3].Name, "extra1")
		require.Equal(t, united.Columns[3].ComplexType, schema.Optional{Item: schema.TypeString})
	})

	t.Run("Test delete optional column", func(t *testing.T) {
		expected2 := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeBytes},
				},
				{
					Name:        "extra1",
					ComplexType: schema.TypeString,
				},
			},
		}

		united, err = unionSchemas(united, expected2)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 4)

		require.Equal(t, united.Columns[0].Name, "key")
		require.Equal(t, united.Columns[0].SortOrder, schema.SortAscending)
		require.Equal(t, united.Columns[0].ComplexType, schema.TypeInt64)

		require.Equal(t, united.Columns[1].Name, "value")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeBytes})

		require.Equal(t, united.Columns[2].Name, "extra1")
		require.Equal(t, united.Columns[2].ComplexType, schema.Optional{Item: schema.TypeString})

		require.Equal(t, united.Columns[3].Name, "extra")
		require.Equal(t, united.Columns[3].ComplexType, schema.Optional{Item: schema.TypeString})
	})

	t.Run("Test rename column(delete and add)", func(t *testing.T) {
		expected3 := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeBytes},
				},
				{
					Name:        "extra2",
					ComplexType: schema.TypeString,
				},
			},
		}

		united, err = unionSchemas(united, expected3)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 5)

		require.Equal(t, united.Columns[0].Name, "key")
		require.Equal(t, united.Columns[0].SortOrder, schema.SortAscending)
		require.Equal(t, united.Columns[0].ComplexType, schema.TypeInt64)

		require.Equal(t, united.Columns[1].Name, "value")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeBytes})

		require.Equal(t, united.Columns[2].Name, "extra2")
		require.Equal(t, united.Columns[2].ComplexType, schema.Optional{Item: schema.TypeString})

		require.Equal(t, united.Columns[3].Name, "extra1")
		require.Equal(t, united.Columns[3].ComplexType, schema.Optional{Item: schema.TypeString})

		require.Equal(t, united.Columns[4].Name, "extra")
		require.Equal(t, united.Columns[4].ComplexType, schema.Optional{Item: schema.TypeString})
	})

	t.Run("Test append column to key and reorder non key columns", func(t *testing.T) {
		expected4 := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "key_extra",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "extra2",
					ComplexType: schema.TypeString,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeBytes},
				},
			},
		}

		united, err = unionSchemas(united, expected4)
		require.NoError(t, err)
		require.True(t, united.UniqueKeys)
		require.True(t, len(united.Columns) == 6)

		require.Equal(t, united.Columns[0].Name, "key")
		require.Equal(t, united.Columns[0].SortOrder, schema.SortAscending)
		require.Equal(t, united.Columns[0].ComplexType, schema.TypeInt64)

		require.Equal(t, united.Columns[1].Name, "key_extra")
		require.Equal(t, united.Columns[1].SortOrder, schema.SortAscending)
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeInt64})

		require.Equal(t, united.Columns[2].Name, "extra2")
		require.Equal(t, united.Columns[2].ComplexType, schema.Optional{Item: schema.TypeString})

		require.Equal(t, united.Columns[3].Name, "value")
		require.Equal(t, united.Columns[3].ComplexType, schema.Optional{Item: schema.TypeBytes})

		require.Equal(t, united.Columns[4].Name, "extra1")
		require.Equal(t, united.Columns[4].ComplexType, schema.Optional{Item: schema.TypeString})

		require.Equal(t, united.Columns[5].Name, "extra")
		require.Equal(t, united.Columns[5].ComplexType, schema.Optional{Item: schema.TypeString})
	})

	t.Run("Test delete key column", func(t *testing.T) {
		expected5 := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "extra2",
					ComplexType: schema.TypeString,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeBytes},
				},
			},
		}
		united, err = unionSchemas(united, expected5)
		require.Error(t, err)
	})

	t.Run("Test uncompatible type change", func(t *testing.T) {
		expected6 := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "key_extra",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "extra2",
					ComplexType: schema.TypeString,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeInt64},
				},
			},
		}
		united, err = unionSchemas(united, expected6)
		require.Error(t, err)
	})

	t.Run("Test append to key existing column", func(t *testing.T) {
		expected7 := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "key_extra",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "extra2",
					ComplexType: schema.TypeString,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.Optional{Item: schema.TypeInt64},
				},
			},
		}
		united, err = unionSchemas(united, expected7)
		require.Error(t, err)
	})

	t.Run("Test append non key column before key", func(t *testing.T) {
		expected8 := schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:        "flag",
					ComplexType: schema.Optional{Item: schema.TypeBoolean},
				},
				{
					Name:        "key",
					ComplexType: schema.TypeInt64,
					SortOrder:   schema.SortAscending,
				},
				{
					Name:        "value",
					ComplexType: schema.TypeString,
				},
				{
					Name:        "extra",
					ComplexType: schema.Optional{Item: schema.TypeString},
				},
			},
		}
		united, err = unionSchemas(actual, expected8)
		require.NoError(t, err)

		require.Equal(t, united.Columns[0].Name, "key")
		require.Equal(t, united.Columns[0].ComplexType, schema.TypeInt64)
		require.Equal(t, united.Columns[0].SortOrder, schema.SortAscending)

		require.Equal(t, united.Columns[1].Name, "flag")
		require.Equal(t, united.Columns[1].ComplexType, schema.Optional{Item: schema.TypeBoolean})

		require.Equal(t, united.Columns[2].Name, "value")
		require.Equal(t, united.Columns[2].ComplexType, schema.TypeString)

		require.Equal(t, united.Columns[3].Name, "extra")
		require.Equal(t, united.Columns[3].ComplexType, schema.Optional{Item: schema.TypeString})
	})
}

func TestCheckForFatalError(t *testing.T) {
	abstract.CheckErrorWrapping(t, "default creation", IsIncompatibleSchemaErr, func(err error) error {
		return NewIncompatibleSchemaErr(err)
	})
	abstract.CheckErrorWrapping(t, "struct", IsIncompatibleSchemaErr, func(err error) error {
		return IncompatibleSchemaErr{error: err}
	})
	abstract.CheckErrorWrapping(t, "pointer", IsIncompatibleSchemaErr, func(err error) error {
		return &IncompatibleSchemaErr{error: err}
	})
}

func TestSchemasAreEqual(t *testing.T) {
	t.Run("equal schemas with shuffled columns", func(t *testing.T) {
		currentSchema := []abstract.ColSchema{
			{ColumnName: "id", PrimaryKey: true, DataType: string(schema.TypeInt64)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
			{ColumnName: "age", DataType: string(schema.TypeInt32)},
			{ColumnName: "is_married", DataType: string(schema.TypeBoolean)},
		}

		receivedSchema := []abstract.ColSchema{
			{ColumnName: "age", DataType: string(schema.TypeInt32)},
			{ColumnName: "id", PrimaryKey: true, DataType: string(schema.TypeInt64)},
			{ColumnName: "is_married", DataType: string(schema.TypeBoolean)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
		}

		require.True(t, schemasAreEqual(currentSchema, receivedSchema))
		require.True(t, schemasAreEqual(receivedSchema, currentSchema))
		require.True(t, schemasAreEqual(currentSchema, currentSchema))
		require.True(t, schemasAreEqual(receivedSchema, receivedSchema))
	})

	t.Run("received schema is subset of current schema", func(t *testing.T) {
		currentSchema := []abstract.ColSchema{
			{ColumnName: "id", PrimaryKey: true, DataType: string(schema.TypeInt64)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
			{ColumnName: "age", DataType: string(schema.TypeInt32)},
			{ColumnName: "is_married", DataType: string(schema.TypeBoolean)},
		}

		receivedSchema := []abstract.ColSchema{
			{ColumnName: "id", PrimaryKey: true, DataType: string(schema.TypeInt64)},
			{ColumnName: "is_married", DataType: string(schema.TypeBoolean)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
		}

		require.False(t, schemasAreEqual(currentSchema, receivedSchema))
		require.False(t, schemasAreEqual(receivedSchema, currentSchema))
		require.True(t, schemasAreEqual(currentSchema, currentSchema))
		require.True(t, schemasAreEqual(receivedSchema, receivedSchema))
	})

	t.Run("in received schema was changed type and system key of primary key", func(t *testing.T) {
		currentSchema := []abstract.ColSchema{
			{ColumnName: "id", PrimaryKey: true, DataType: string(schema.TypeInt32)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
			{ColumnName: "is_married", DataType: string(schema.TypeBoolean)},
		}

		receivedSchema := []abstract.ColSchema{
			{ColumnName: "id", PrimaryKey: true, DataType: string(schema.TypeInt64)},
			{ColumnName: "is_married", DataType: string(schema.TypeBoolean)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
		}

		require.False(t, schemasAreEqual(currentSchema, receivedSchema))
		require.False(t, schemasAreEqual(receivedSchema, currentSchema))
		require.True(t, schemasAreEqual(currentSchema, currentSchema))
		require.True(t, schemasAreEqual(receivedSchema, receivedSchema))
	})

	t.Run("repeating columns in received schema", func(t *testing.T) {
		currentSchema := []abstract.ColSchema{
			{ColumnName: "id", PrimaryKey: true, DataType: string(schema.TypeInt32)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
			{ColumnName: "is_married", DataType: string(schema.TypeBoolean)},
		}

		receivedSchema := []abstract.ColSchema{
			{ColumnName: "id", PrimaryKey: true, DataType: string(schema.TypeInt64)},
			{ColumnName: "is_married", DataType: string(schema.TypeBoolean)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
			{ColumnName: "name", DataType: string(schema.TypeString)},
		}

		require.False(t, schemasAreEqual(currentSchema, receivedSchema))
	})
}

func TestFitTimeToYT(t *testing.T) {
	ytMinTime, _ := time.Parse(time.RFC3339Nano, "1970-01-01T00:00:00.000000Z")
	ytMaxTime, _ := time.Parse(time.RFC3339Nano, "2105-12-31T23:59:59.999999Z")
	beforeMinTime := ytMinTime.Add(-time.Hour * 240)
	afterMaxTime := ytMaxTime.Add(time.Hour * 240)
	now := time.Now()

	t.Run("Timestamp", func(t *testing.T) {
		minTimestamp, err1 := schema.NewTimestamp(ytMinTime)
		maxTimestamp, err2 := schema.NewTimestamp(ytMaxTime)
		nowTimestamp, err3 := schema.NewTimestamp(now)
		require.Equal(t, []error{nil, nil, nil}, []error{err1, err2, err3})

		res, err := castTimeWithDataLoss(beforeMinTime, schema.NewTimestamp)
		require.NoError(t, err)
		require.Equal(t, minTimestamp, res) // Before min time is rounded to min time.

		res, err = castTimeWithDataLoss(afterMaxTime, schema.NewTimestamp)
		require.NoError(t, err)
		require.Equal(t, maxTimestamp, res) // After max time is rounded to max time.

		res, err = castTimeWithDataLoss(now, schema.NewTimestamp)
		require.NoError(t, err)
		require.Equal(t, nowTimestamp, res) // Now time is not changed.
	})

	t.Run("Date", func(t *testing.T) {
		minDate, err1 := schema.NewDate(ytMinTime)
		maxDate, err2 := schema.NewDate(ytMaxTime)
		nowDate, err3 := schema.NewDate(now)
		require.Equal(t, []error{nil, nil, nil}, []error{err1, err2, err3})

		res, err := castTimeWithDataLoss(beforeMinTime, schema.NewDate)
		require.NoError(t, err)
		require.Equal(t, minDate, res) // Before min time is rounded to min time.

		res, err = castTimeWithDataLoss(afterMaxTime, schema.NewDate)
		require.NoError(t, err)
		require.Equal(t, maxDate, res) // After max time is rounded to max time.

		res, err = castTimeWithDataLoss(now, schema.NewDate)
		require.NoError(t, err)
		require.Equal(t, nowDate, res) // Now time is not changed.
	})

	t.Run("Datetime", func(t *testing.T) {
		minDatetime, err1 := schema.NewDatetime(ytMinTime)
		maxDatetime, err2 := schema.NewDatetime(ytMaxTime)
		nowDatetime, err3 := schema.NewDatetime(now)
		require.Equal(t, []error{nil, nil, nil}, []error{err1, err2, err3})

		res, err := castTimeWithDataLoss(beforeMinTime, schema.NewDatetime)
		require.NoError(t, err)
		require.Equal(t, minDatetime, res) // Before min time is rounded to min time.

		res, err = castTimeWithDataLoss(afterMaxTime, schema.NewDatetime)
		require.NoError(t, err)
		require.Equal(t, maxDatetime, res) // After max time is rounded to max time.

		res, err = castTimeWithDataLoss(now, schema.NewDatetime)
		require.NoError(t, err)
		require.Equal(t, nowDatetime, res) // Now time is not changed.
	})
}
