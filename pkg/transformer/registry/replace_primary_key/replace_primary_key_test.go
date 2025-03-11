package replaceprimarykey

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
)

type colParams struct {
	Name         string
	Value        string
	IsPrimaryKey bool
}

func makeDummyChangeItemWithSchema(params []colParams) abstract.ChangeItem {
	chI := new(abstract.ChangeItem)
	var names []string
	var values []interface{}
	var schema []abstract.ColSchema
	for _, param := range params {
		schema = append(schema, abstract.ColSchema{
			PrimaryKey: param.IsPrimaryKey,
			ColumnName: param.Name,
			DataType:   "string",
		})
		names = append(names, param.Name)
		values = append(values, param.Value)
	}
	chI.TableSchema = abstract.NewTableSchema(schema)
	chI.ColumnNames = names
	chI.ColumnValues = values
	return *chI
}

func TestReplacePrimaryKeyTransformer(t *testing.T) {
	t.Parallel()
	_, err := NewReplacePrimaryKeyTransformer(Config{
		Keys: []string{"key1", "key1"},
	})
	require.Error(t, err)

	_, err = NewReplacePrimaryKeyTransformer(Config{
		Keys: []string{"key3", "key2"},
	})
	require.NoError(t, err)

	transformer, _ := NewReplacePrimaryKeyTransformer(Config{
		Keys: []string{"col1", "col2"},
	})

	col1Key := colParams{Name: "col1", IsPrimaryKey: true, Value: "1"}
	col2Key := colParams{Name: "col2", IsPrimaryKey: true, Value: "2"}
	col3Key := colParams{Name: "col3", IsPrimaryKey: true, Value: "3"}
	col1NotKey := colParams{Name: "col1", IsPrimaryKey: false, Value: "1"}
	col2NotKey := colParams{Name: "col2", IsPrimaryKey: false, Value: "2"}
	col3NotKey := colParams{Name: "col3", IsPrimaryKey: false, Value: "3"}

	testChangeItems := []abstract.ChangeItem{
		makeDummyChangeItemWithSchema([]colParams{col1NotKey, col2NotKey, col3NotKey}),
		makeDummyChangeItemWithSchema([]colParams{col2Key, col1NotKey, col3Key}),
		makeDummyChangeItemWithSchema([]colParams{col1NotKey, col3NotKey}),
		makeDummyChangeItemWithSchema([]colParams{col3NotKey, col1Key}),
	}
	require.True(t, transformer.Suitable(abstract.TableID{}, testChangeItems[0].TableSchema))
	require.True(t, transformer.Suitable(abstract.TableID{}, testChangeItems[1].TableSchema))
	require.False(t, transformer.Suitable(abstract.TableID{}, testChangeItems[2].TableSchema))
	require.False(t, transformer.Suitable(abstract.TableID{}, testChangeItems[3].TableSchema))

	var resultChangeItems []abstract.ChangeItem

	for _, item := range testChangeItems {
		transformResult := transformer.Apply([]abstract.ChangeItem{item})
		if transformer.Suitable(abstract.TableID{}, item.TableSchema) {
			for i := range transformer.Keys {
				require.Equal(t, transformer.Keys[i], transformResult.Transformed[0].TableSchema.Columns()[i].ColumnName)
				require.True(t, transformResult.Transformed[0].TableSchema.Columns()[i].PrimaryKey)
			}
		}
		resultChangeItems = append(resultChangeItems, transformResult.Transformed...)
	}

	for chItemNumber, chI := range testChangeItems {
		var valuesByColName = make(map[string]interface{})
		for i, name := range chI.ColumnNames {
			valuesByColName[name] = chI.ColumnValues[i]
		}
		for i, name := range resultChangeItems[chItemNumber].ColumnNames {
			require.Equal(t, valuesByColName[name], resultChangeItems[chItemNumber].ColumnValues[i])
		}
	}
}

func TestReplacePrimaryKeyTransformerOldKeys(t *testing.T) {
	transformer, _ := NewReplacePrimaryKeyTransformer(Config{
		Keys: []string{"col1", "col2"},
	})

	col1 := colParams{Name: "col1", IsPrimaryKey: false, Value: "1"}
	col2 := colParams{Name: "col2", IsPrimaryKey: false, Value: "2"}
	col3 := colParams{Name: "col3", IsPrimaryKey: true, Value: "3"}

	chItemInsert := makeDummyChangeItemWithSchema([]colParams{col1, col2, col3})
	chItemUpdate := makeDummyChangeItemWithSchema([]colParams{col1, col2, col3})
	chItemUpdate.Kind = abstract.UpdateKind

	require.True(t, transformer.Suitable(abstract.TableID{}, chItemInsert.TableSchema))
	require.True(t, transformer.Suitable(abstract.TableID{}, chItemUpdate.TableSchema))

	transformResultInsert := transformer.Apply([]abstract.ChangeItem{chItemInsert})
	require.Empty(t, transformResultInsert.Transformed[0].OldKeys.KeyTypes)
	require.Empty(t, transformResultInsert.Transformed[0].OldKeys.KeyNames)
	require.Empty(t, transformResultInsert.Transformed[0].OldKeys.KeyValues)

	transformResultUpdate := transformer.Apply([]abstract.ChangeItem{chItemUpdate})
	require.Equal(t, []string{"string", "string"}, transformResultUpdate.Transformed[0].OldKeys.KeyTypes)
	require.Equal(t, []string{"col1", "col2"}, transformResultUpdate.Transformed[0].OldKeys.KeyNames)
	require.Equal(t, []interface{}{"1", "2"}, transformResultUpdate.Transformed[0].OldKeys.KeyValues)
}
