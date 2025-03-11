package rawdocgrouper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/pkg/util/set"
)

func TestRawDocGroupTransformer(t *testing.T) {
	t.Parallel()

	t.Run("duplicate keys fail", func(t *testing.T) {
		_, err := NewRawDocGroupTransformer(RawDocGrouperConfig{
			Keys: []string{"key1", "key1"},
		})
		require.Error(t, err)
	})

	t.Run("different keys ok", func(t *testing.T) {
		_, err := NewRawDocGroupTransformer(RawDocGrouperConfig{
			Keys: []string{"key3", "key2"},
		})
		require.NoError(t, err)
	})

	t.Run("Suitable check by table and fields", func(t *testing.T) {
		transformer, _ := NewRawDocGroupTransformer(RawDocGrouperConfig{
			Tables: filter.Tables{IncludeTables: []string{"table1"}},
			Keys:   []string{"col1", "col2"},
			Fields: []string{"col4"},
		})

		item := dummyItemWithKindAndTable([]colParams{col1NotKey, col2NotKey, col3NotKey, col4NotKey}, abstract.UpdateKind, "table1")
		require.True(t, transformer.Suitable(item.TableID(), item.TableSchema))

		item = dummyItemWithKindAndTable([]colParams{col1NotKey, col2NotKey, col3NotKey, col4NotKey}, abstract.UpdateKind, "table2")
		require.False(t, transformer.Suitable(item.TableID(), item.TableSchema))

		item = dummyItemWithKindAndTable([]colParams{col2Key, col1NotKey, col3Key, col4NotKey}, abstract.UpdateKind, "table1")
		require.True(t, transformer.Suitable(item.TableID(), item.TableSchema))

		item = dummyItemWithKindAndTable([]colParams{col2Key, col1NotKey, col3Key}, abstract.UpdateKind, "table1")
		require.False(t, transformer.Suitable(item.TableID(), item.TableSchema))

		item = dummyItemWithKindAndTable([]colParams{col1NotKey, col3NotKey, col4NotKey}, abstract.UpdateKind, "table1")
		require.False(t, transformer.Suitable(item.TableID(), item.TableSchema))

		item = dummyItemWithKindAndTable([]colParams{col3NotKey, col1Key}, abstract.UpdateKind, "table1")
		require.False(t, transformer.Suitable(item.TableID(), item.TableSchema))
	})

	t.Run("Wrong columns fail transfer", func(t *testing.T) {
		transformer, _ := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
			Tables: filter.Tables{IncludeTables: []string{"table1"}},
			Keys:   []string{"col1", "col2"},
			Fields: []string{"col3"},
		})

		testChangeItems := []abstract.ChangeItem{
			dummyItem([]colParams{col1NotKey, col3NotKey}),
			dummyItem([]colParams{col1Key, col2Key}),
		}

		for _, item := range testChangeItems {
			require.False(t, transformer.Suitable(abstract.TableID{}, item.TableSchema))
			transformResult := transformer.Apply([]abstract.ChangeItem{item})
			require.Equal(t, 0, len(transformResult.Transformed), "Unsuitable data should not be parsed!")
			require.Equal(t, 1, len(transformResult.Errors), "Error should be returned!!")
		}
	})

	t.Run("Values are parsed", func(t *testing.T) {
		transformer, _ := NewRawDocGroupTransformer(RawDocGrouperConfig{
			Keys:   []string{"col1", "col2"},
			Fields: []string{"col3"},
		})

		testChangeItems := []abstract.ChangeItem{
			dummyItem([]colParams{col1NotKey, col2NotKey, col3NotKey, col4NotKey, col5NotKey, restCol}),  //schema 1
			dummyItem([]colParams{col1NotKey, col2NotKey2, col3NotKey, col4NotKey, col5NotKey, restCol}), //same schema 1, diff values
			dummyItem([]colParams{col2Key, col1NotKey, col3Key, restCol}),                                // schema 2
		}

		for _, testItem := range testChangeItems {
			transformed := transformer.Apply([]abstract.ChangeItem{testItem}).Transformed
			resultItem := transformAndCheckFields(t, transformed, transformer.Keys, transformer.Fields, false)

			if !transformer.Suitable(abstract.TableID{}, testItem.TableSchema) {
				break
			}

			compareColumns(t, resultItem, []string{"col1", "col2", "col3", "etl_updated_at", "doc"})
			valuesByColName := getExpectedValues(testItem)

			for i, name := range resultItem.ColumnNames {
				require.Equal(t, valuesByColName[name], resultItem.ColumnValues[i],
					"Field %s has unexpected value %s, expected: %s",
					name, resultItem.ColumnValues[i], valuesByColName[name])
			}
		}
	})

	t.Run("Different schemas are processed", func(t *testing.T) {
		transformer, _ := NewRawDocGroupTransformer(RawDocGrouperConfig{
			Keys:   []string{"col1", "col2"},
			Fields: []string{"col3"},
		})

		testChangeItems := []abstract.ChangeItem{
			dummyItem([]colParams{col1NotKey, col2NotKey, col3NotKey, col4NotKey, col5NotKey, restCol}),   //schema 1
			dummyItem([]colParams{col1NotKey, col2NotKey2, col3NotKey, col4NotKey, col5NotKey, restCol}),  //schema 1, diff values
			dummyItem([]colParams{col1NotKey, col2NotKey2, col3NotKey, col4NotKey2, col5NotKey, restCol}), //schema 1, diff values
			dummyItem([]colParams{col1NotKey2, col2NotKey2, col3NotKey, col4NotKey, col5NotKey, restCol}), //schema 2 - diff type of col2
			dummyItem([]colParams{col2Key, col1NotKey, col3Key, restCol}),                                 //schema 3
		}

		differentSchemas := make([]string, 0)
		differentTargetSchemas := make([]*abstract.TableSchema, 0)

		for _, testItem := range testChangeItems {
			transformed := transformer.Apply([]abstract.ChangeItem{testItem}).Transformed
			resultItem := transformAndCheckFields(t, transformed, transformer.Keys, transformer.Fields, false)

			differentTargetSchemas = append(differentTargetSchemas, resultItem.TableSchema)
			if !transformer.Suitable(abstract.TableID{}, testItem.TableSchema) {
				break
			}
			hash, _ := testItem.TableSchema.Hash()
			differentSchemas = append(differentSchemas, hash)
			compareColumns(t, resultItem, []string{"col1", "col2", "col3", "etl_updated_at", "doc"})
		}
		require.Equal(t, set.New[string](differentSchemas...).Len(), 3, "Different schema count mismatch, some changeItems should have same schemas!")
		require.Equal(t, set.New[string](differentSchemas...).Len(), set.New[*abstract.TableSchema](differentTargetSchemas...).Len(), "input and output schema count mismatch!!")
	})

	t.Run("System events schema fixed", func(t *testing.T) {
		transformer, _ := NewRawDocGroupTransformer(RawDocGrouperConfig{
			Keys: []string{"col1", "col2"},
		})

		testChangeItems := []abstract.ChangeItem{
			dummyItemWithKind([]colParams{col1NotKey, col2NotKey}, abstract.DoneTableLoad),
			dummyItemWithKind([]colParams{col1Key, col2Key}, abstract.InitTableLoad),
		}

		for _, item := range testChangeItems {
			require.True(t, transformer.Suitable(abstract.TableID{}, item.TableSchema))
			transformResult := transformer.Apply([]abstract.ChangeItem{item})
			require.Equal(t, len(transformResult.Errors), 0, "System chItems data should not cause Errors!")
			require.Equal(t, len(transformResult.Transformed), 1, "Original chItems should be returned!!")

			columnsFromSchema := make([]string, 0, transformer.keySet.Len())
			for _, ch := range transformResult.Transformed[0].TableSchema.Columns() {
				columnsFromSchema = append(columnsFromSchema, ch.ColumnName)
			}
			require.Equal(t, columnsFromSchema,
				[]string{"col1", "col2", "etl_updated_at", "doc"}, "Changed schema chItems columns be returned!!")
		}
	})

	t.Run("Update for not original key field", func(t *testing.T) {
		transformer, _ := NewRawDocGroupTransformer(RawDocGrouperConfig{
			Keys:                []string{"col1", "col2"},
			ShouldUpdateOldKeys: true,
		})

		item := dummyItemWithKind([]colParams{col1NotKey, col2NotKey, col3Key}, abstract.UpdateKind)
		item.OldKeys = abstract.OldKeysType{
			KeyNames:  []string{"col3"},
			KeyValues: []interface{}{"oldCol3"},
		}

		itemHash, err := item.TableSchema.Hash()
		require.NoError(t, err)

		transformResult := transformer.Apply([]abstract.ChangeItem{item})
		require.Equal(t, len(transformResult.Errors), 0, "System chItems data should not cause Errors!")
		require.Equal(t, len(transformResult.Transformed), 1, "Original chItems should be returned!!")
		require.ElementsMatch(t, transformer.additionalKeys[itemHash], []string{"col1", "col2"})

		resultItem := transformResult.Transformed[0]

		require.False(t, resultItem.KeysChanged())
		oldKeys := resultItem.OldKeys
		require.ElementsMatch(t, resultItem.KeyCols(), resultItem.OldKeys.KeyNames)
		columnNameIndices := resultItem.ColumnNameIndices()
		for i, name := range oldKeys.KeyNames {
			require.Equal(t, resultItem.ColumnValues[columnNameIndices[name]], oldKeys.KeyValues[i])
		}
		require.ElementsMatch(t, transformer.additionalKeys[itemHash], []string{"col1", "col2"})
	})
}

func getExpectedValues(changeItem abstract.ChangeItem) map[string]interface{} {
	var valuesByColName = make(map[string]interface{})
	var docField = make(map[string]interface{})

	for i, name := range changeItem.ColumnNames {
		if name == restField {
			docField["innerKey1"] = "1"
			docField["innerKey2"] = 2
			docField["innerKey3"] = true
		} else {
			docField[name] = changeItem.ColumnValues[i]
		}
		if name == "col1" || name == "col2" || name == "col3" {
			valuesByColName[name] = changeItem.ColumnValues[i]
		}
	}
	valuesByColName[etlUpdatedField] = time.Unix(0, int64(updateTime))
	valuesByColName[rawDataField] = docField
	return valuesByColName
}
