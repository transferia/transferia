package rawdocgrouper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/pkg/util/set"
)

func TestRawCdcDocGroupTransformer(t *testing.T) {
	t.Parallel()
	t.Run("duplicate keys fail", func(t *testing.T) {
		_, err := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
			Keys: []string{"key1", "key1"},
		})
		require.Error(t, err)
	})

	t.Run("duplicate keys and non-keys fail", func(t *testing.T) {
		_, err := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
			Keys:   []string{"key1", "key3"},
			Fields: []string{"key1"},
		})
		require.Error(t, err)
	})

	t.Run("different keys and special fields ok", func(t *testing.T) {
		_, err := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
			Keys:   []string{"key3", "key2"},
			Fields: []string{"etl_updated_at"},
		})
		require.NoError(t, err)
	})

	t.Run("Suitable check by tables", func(t *testing.T) {
		transformer, _ := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
			Tables: filter.Tables{IncludeTables: []string{"table1"}},
			Keys:   []string{"col1", "col2", "etl_updated_at"},
			Fields: []string{"col4", "doc"},
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
			dummyItem([]colParams{col2Key}),
		}

		for _, item := range testChangeItems {
			require.False(t, transformer.Suitable(abstract.TableID{}, item.TableSchema))
			transformResult := transformer.Apply([]abstract.ChangeItem{item})
			require.Equal(t, 0, len(transformResult.Transformed), "Unsuitable data should not be parsed!")
			require.Equal(t, 1, len(transformResult.Errors), "Error should be returned!!")
		}
	})

	t.Run("All keys are present on delete", func(t *testing.T) {
		transformer, _ := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
			Tables: filter.Tables{IncludeTables: []string{"table1"}},
			Keys:   []string{"col1", "col2"},
		})
		item := dummyItemWithKindAndTable([]colParams{col1Key, col2NotKey}, abstract.DeleteKind, "table1")
		require.True(t, transformer.Suitable(item.TableID(), item.TableSchema))

		transformResult := transformer.Apply([]abstract.ChangeItem{item})
		require.Equal(t, 1, len(transformResult.Transformed), "Some data was not transformed!")
		require.Equal(t, 0, len(transformResult.Errors), "No Error expected!!")
		require.Contains(t, transformResult.Transformed[0].ColumnNames, "col2", "Some keys are missing!")
	})

	t.Run("Values are parsed", func(t *testing.T) {
		transformer, _ := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
			Keys:   []string{"col1", "col2"},
			Fields: []string{"col3"},
		})

		testChangeItems := []abstract.ChangeItem{
			dummyItemWithKind([]colParams{col1NotKey, col2NotKey, col3NotKey, col4NotKey, col5NotKey}, abstract.DeleteKind),
			dummyItemWithKind([]colParams{col2Key, col1NotKey, col3Key, col4NotKey, col5NotKey}, abstract.UpdateKind),
		}

		for chItemNumber, testItem := range testChangeItems {
			transformed := transformer.Apply([]abstract.ChangeItem{testItem}).Transformed
			resultItem := transformAndCheckFields(t, transformed, transformer.Keys, transformer.Fields, true)

			if !transformer.Suitable(abstract.TableID{}, testItem.TableSchema) {
				break
			}

			require.Equal(t, resultItem.Kind, abstract.InsertKind, "All transformed data should be of insert kind!")
			expectedCols := []string{"etl_updated_at", "col1", "col2", "deleted_flg", "doc"}
			if testItem.Kind != abstract.DeleteKind {
				expectedCols = append(expectedCols, "col3")
			}
			compareColumns(t, resultItem, expectedCols)

			valuesByColName := getExpectedValuesCdc(testItem, chItemNumber)

			for i, name := range resultItem.ColumnNames {
				require.Equal(t, valuesByColName[name], resultItem.ColumnValues[i],
					"Field %s has unexpected value %s, expected: %s",
					name, resultItem.ColumnValues[i], valuesByColName[name])
			}
		}
	})

	t.Run("Different schemas are processed", func(t *testing.T) {
		transformer, _ := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
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
			resultItem := transformAndCheckFields(t, transformed, transformer.Keys, transformer.Fields, true)

			differentTargetSchemas = append(differentTargetSchemas, resultItem.TableSchema)
			if !transformer.Suitable(abstract.TableID{}, testItem.TableSchema) {
				break
			}
			hash, _ := testItem.TableSchema.Hash()
			differentSchemas = append(differentSchemas, hash)
			compareColumns(t, resultItem, []string{"etl_updated_at", "col1", "col2", "col3", "deleted_flg", "doc"})
		}
		require.Equal(t, set.New[string](differentSchemas...).Len(), 3, "Different schema count mismatch, some changeItems should have same schemas!")
		require.Equal(t, set.New[string](differentSchemas...).Len(), set.New[*abstract.TableSchema](differentTargetSchemas...).Len(), "input and output schema count mismatch!!")
	})

	t.Run("System events schema fixed", func(t *testing.T) {
		transformer, _ := NewCdcHistoryGroupTransformer(RawCDCDocGrouperConfig{
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

			columnsFromSchema := make([]string, 0, transformer.keySet.Len()+len(transformer.Fields))
			for _, ch := range transformResult.Transformed[0].TableSchema.Columns() {
				columnsFromSchema = append(columnsFromSchema, ch.ColumnName)
			}
			require.Equal(t, []string{"etl_updated_at", "col1", "col2", "deleted_flg", "doc"},
				columnsFromSchema, "Changed schema chItems columns be returned!!")
		}
	})
}

func getExpectedValuesCdc(chI abstract.ChangeItem, chItemNumber int) map[string]interface{} {
	var valuesByColName = make(map[string]interface{})
	var docField = make(map[string]interface{})

	var colNames []string
	var colvalues []interface{}
	if chI.Kind == abstract.DeleteKind {
		colNames = chI.OldKeys.KeyNames
		colvalues = chI.OldKeys.KeyValues
	} else {
		colNames = chI.ColumnNames
		colvalues = chI.ColumnValues
	}
	for i, name := range colNames {
		docField[name] = colvalues[i]
		if name == "col1" || name == "col2" || name == "col3" {
			valuesByColName[name] = colvalues[i]
		}
	}
	valuesByColName[etlUpdatedField] = time.Unix(0, int64(updateTime))
	valuesByColName[rawDataField] = docField
	valuesByColName[deletedField] = chItemNumber == 0 //chItem 0 is DeleteKind
	return valuesByColName
}
