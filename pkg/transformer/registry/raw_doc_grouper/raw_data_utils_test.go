package rawdocgrouper

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

type oldKey struct {
	keyName  string
	keyValue interface{}
}

func TestCollectAdditionalKeysForTransformer(t *testing.T) {
	testCases := []struct {
		testName           string
		newKeys            []string
		originalKeyColumns []string
		expected           []string
	}{
		{"The original keys are equivalent to the new keys", []string{"col1", "col2", "col3"}, []string{"col1", "col2", "col3"}, []string{}},
		{"No new keys", []string{}, []string{"col1", "col2", "col3"}, []string{}},
		{"None of the new ones match the original keys", []string{"col1", "col2"}, []string{"col3", "col4"}, []string{"col1", "col2"}},
		{"Some new keys match with original keys", []string{"col1", "col2"}, []string{"col2", "col3"}, []string{"col1"}},
		{"New keys include original keys", []string{"col1", "col2", "col3", "col4"}, []string{"col1", "col2"}, []string{"col3", "col4"}},
	}
	for _, tt := range testCases {
		t.Run(tt.testName, func(t *testing.T) {
			require.Equal(t, tt.expected, CollectAdditionalKeysForTransformer(tt.newKeys, tt.originalKeyColumns))
		})
	}
}

func TestProcessUpdateItem(t *testing.T) {
	additionalKeys := make(map[string][]string)

	addItemToAdditionalKeys := func(item abstract.ChangeItem, newKeys []string, additionalKeys map[string][]string) {
		hash, err := item.TableSchema.Hash()
		require.NoError(t, err)
		additionalKeys[hash] = newKeys
	}

	processUpdateItemAndCalculateHash := func(item abstract.ChangeItem, additionalKeys map[string][]string, addAdditionalKeys bool) abstract.ChangeItem {
		hash, err := item.TableSchema.Hash()
		require.NoError(t, err)
		require.NotEmpty(t, hash)

		updatedItem := processUpdateItem(item, additionalKeys[hash], addAdditionalKeys)
		logger.Log.Infof("andreysss::updatedItem oldKeys : %v", updatedItem.OldKeys.KeyNames)
		return updatedItem
	}

	addOldKey := func(prevOldKeys abstract.OldKeysType, oldKey ...oldKey) abstract.OldKeysType {
		var oldKeys abstract.OldKeysType
		copy(oldKeys.KeyNames, prevOldKeys.KeyNames)
		copy(oldKeys.KeyValues, prevOldKeys.KeyValues)
		copy(oldKeys.KeyTypes, prevOldKeys.KeyTypes)

		for _, key := range oldKey {
			oldKeys.KeyNames = append(oldKeys.KeyNames, key.keyName)
			oldKeys.KeyValues = append(oldKeys.KeyValues, key.keyValue)
		}

		return oldKeys
	}

	t.Run("not existed schema hash", func(t *testing.T) {
		item := dummyItemWithKind([]colParams{col1Key, col1NotKey2}, abstract.UpdateKind)
		require.Equal(t, item, processUpdateItemAndCalculateHash(item, additionalKeys, true))
	})

	t.Run("not update item", func(t *testing.T) {
		item := dummyItemWithKind([]colParams{col1Key, col1NotKey2}, changeitem.InsertKind)
		addItemToAdditionalKeys(item, []string{}, additionalKeys)
		require.Equal(t, item, processUpdateItemAndCalculateHash(item, additionalKeys, true))

		item = dummyItemWithKind([]colParams{col1Key, col2NotKey}, changeitem.DeleteKind)
		addItemToAdditionalKeys(item, []string{"col2"}, additionalKeys)
		require.Equal(t, item, processUpdateItemAndCalculateHash(item, additionalKeys, true))
	})

	t.Run("update item with old keys without updated primary key", func(t *testing.T) {
		item := dummyItemWithKind([]colParams{col1Key, col2Key}, abstract.UpdateKind)
		item.OldKeys = addOldKey(item.OldKeys, oldKey{col1Key.Name, col1Key.Value})
		addItemToAdditionalKeys(item, []string{col2NotKey.Name}, additionalKeys)

		expectedItem := dummyItemWithKind([]colParams{col1Key, col2Key}, abstract.UpdateKind)
		expectedItem.OldKeys = addOldKey(item.OldKeys, []oldKey{{col1Key.Name, col1Key.Value}, {col2NotKey.Name, col2NotKey.Value}}...)
		_, err := expectedItem.TableSchema.Hash()
		require.NoError(t, err)

		newItem := processUpdateItemAndCalculateHash(item, additionalKeys, true)
		require.Equal(t, expectedItem, newItem)
		require.False(t, newItem.KeysChanged())
	})

	t.Run("update item with old keys with updated primary key", func(t *testing.T) {
		item := dummyItemWithKind([]colParams{col1Key, col2NotKey}, abstract.UpdateKind)
		item.OldKeys = addOldKey(item.OldKeys, oldKey{col1Key.Name, "2"})

		addItemToAdditionalKeys(item, []string{col2NotKey.Name}, additionalKeys)

		expectedItem := dummyItemWithKind([]colParams{col1Key, col2NotKey}, abstract.UpdateKind)
		expectedItem.OldKeys = addOldKey(expectedItem.OldKeys, oldKey{col1Key.Name, "2"})
		_, err := expectedItem.TableSchema.Hash()
		require.NoError(t, err)

		newItem := processUpdateItemAndCalculateHash(item, additionalKeys, true)
		require.Equal(t, expectedItem, newItem)
		require.True(t, newItem.KeysChanged())
	})

	t.Run("update item with old key with redundant old key", func(t *testing.T) {
		item := dummyItemWithKind([]colParams{col1Key, col2NotKey}, abstract.UpdateKind)
		item.OldKeys = addOldKey(item.OldKeys, []oldKey{{col1Key.Name, col1Key.Value}, {col3Key.Name, col3Key.Value}}...)
		addItemToAdditionalKeys(item, []string{col2NotKey.Name}, additionalKeys)

		expectedItem := dummyItemWithKind([]colParams{col1Key, col2NotKey}, abstract.UpdateKind)
		expectedItem.OldKeys = addOldKey(item.OldKeys, []oldKey{{col1Key.Name, col1Key.Value}, {col2NotKey.Name, col2NotKey.Value}}...)
		_, err := expectedItem.TableSchema.Hash()
		require.NoError(t, err)

		newItem := processUpdateItemAndCalculateHash(item, additionalKeys, true)
		require.Equal(t, expectedItem, newItem)
		require.NotContains(t, item.OldKeys.KeyNames, "col3")
		require.False(t, newItem.KeysChanged())
	})

	t.Run("update item without adding additional keys", func(t *testing.T) {
		item := dummyItemWithKind([]colParams{col1Key, col2Key}, abstract.UpdateKind)
		item.OldKeys = addOldKey(item.OldKeys, []oldKey{{col1Key.Name, col1Key.Value}, {col3Key.Name, col3Key.Value}, {col4NotKey.Name, col4NotKey.Value}}...)
		addItemToAdditionalKeys(item, []string{col2Key.Name}, additionalKeys)

		expectedItem := dummyItemWithKind([]colParams{col1Key, col2Key}, abstract.UpdateKind)
		expectedItem.OldKeys = addOldKey(expectedItem.OldKeys, oldKey{col1Key.Name, col1Key.Value})
		_, err := expectedItem.TableSchema.Hash()
		require.NoError(t, err)

		newItem := processUpdateItemAndCalculateHash(item, additionalKeys, false)
		require.Equal(t, expectedItem, newItem)
		require.True(t, newItem.KeysChanged())
	})
}
