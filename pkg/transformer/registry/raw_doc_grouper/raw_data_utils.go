package rawdocgrouper

import (
	"reflect"
	"strings"

	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/util/set"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/exp/slices"
)

const etlUpdatedField = "etl_updated_at"
const rawDataField = "doc"
const deletedField = "deleted_flg"

const pgPrefix = "pg:"
const pgText = "pg:text"
const pgTimestamp = "pg:timestamp without time zone"
const pgJSON = "pg:json"

func MakeStubFieldWithOriginalType(colName string, ytType schema.Type, primaryKey bool, original abstract.TableColumns) abstract.ColSchema {
	field := abstract.MakeTypedColSchema(colName, ytType.String(), primaryKey)
	originalType := getOriginalType(original, ytType)
	if originalType != "" {
		field.OriginalType = originalType
	}
	return field
}

func getOriginalType(original abstract.TableColumns, ytType schema.Type) string {
	for _, column := range original {
		//checking if data is from pg and at least 1 original type is present
		//in this case setting pg original for our column
		if strings.Contains(column.OriginalType, pgPrefix) {
			return getPgTypeFor(ytType)
		}
	}
	return ""
}

func getPgTypeFor(ytType schema.Type) string {
	if ytType == schema.TypeAny {
		return pgJSON
	}

	if ytType == schema.TypeDatetime || ytType == schema.TypeTimestamp {
		return pgTimestamp
	}
	//may be throw exception here? then need to change protocol
	return pgText
}

func CollectFieldsForTransformer(fields []string, columns abstract.TableColumns, isKey bool, colNameToIdx map[string]int,
	fieldsMap map[string]schema.Type) []abstract.ColSchema {
	result := make([]abstract.ColSchema, 0, len(fields))
	for _, key := range fields {
		var keySchema abstract.ColSchema
		if slices.Contains(columns.ColumnNames(), key) {
			keySchema = columns[colNameToIdx[key]]
			keySchema.PrimaryKey = isKey
			result = append(result, keySchema)
		} else if fieldType, ok := fieldsMap[key]; ok {
			keySchema = MakeStubFieldWithOriginalType(key, fieldType, isKey, columns)
			result = append(result, keySchema)
		}
	}
	return result
}

// CollectAdditionalKeysForTransformer collecting additional keys that are not keys in the original table schema
func CollectAdditionalKeysForTransformer(newKeys []string, originalKeyColumns []string) []string {
	additionalKeys := make([]string, 0)
	for _, newKey := range newKeys {
		if !slices.Contains(originalKeyColumns, newKey) {
			additionalKeys = append(additionalKeys, newKey)
		}
	}
	return additionalKeys
}

// updateItemProcessing deleting unnecessary fields from oldKeys and add new oldKeys for update.
// Usually, the OldKeys value is not filled in for columns that are not initially keyed,
// which leads to the fact that after applying the raw_doc_crouper, changeItem.KeysChanged() returns true.
// If default keys changed than we shouldn't to add new keys, because this will be an incorrect update.
// We assume that the additional key changes only when the original one is updated.
func processUpdateItem(item abstract.ChangeItem, additionalKeys []string, addAdditionalKeys bool) abstract.ChangeItem {
	if item.Kind != abstract.UpdateKind {
		return item
	}

	inPlaceRemoveOldKeys(&item.OldKeys, item.ColumnNames)
	if !addAdditionalKeys {
		return item
	}

	columnNameIndices := item.ColumnNameIndices()
	if keysChanged(item.OldKeys, item, columnNameIndices) {
		return item
	}

	for _, additionalKey := range additionalKeys {
		if slices.Contains(item.OldKeys.KeyNames, additionalKey) {
			continue
		}

		item.OldKeys.KeyNames = append(item.OldKeys.KeyNames, additionalKey)
		columnIdx := columnNameIndices[additionalKey]
		item.OldKeys.KeyValues = append(item.OldKeys.KeyValues, item.ColumnValues[columnIdx])
	}

	return item
}

// keysChanged compare the old keys with the current keys, and return true only if there is a new key != old key, but if the old keys don't contain any key, it's okay not to check it.
func keysChanged(oldKeysType abstract.OldKeysType, item abstract.ChangeItem, columnNameIndices map[string]int) bool {
	for i, keyName := range oldKeysType.KeyNames {
		columnIdx := columnNameIndices[keyName]
		if !reflect.DeepEqual(oldKeysType.KeyValues[i], item.ColumnValues[columnIdx]) {
			return true
		}
	}

	return false
}

func inPlaceRemoveOldKeys(oldKeysType *abstract.OldKeysType, columnNames []string) {
	if len(oldKeysType.KeyNames) != len(oldKeysType.KeyValues) {
		return
	}

	n := 0
	for i := range oldKeysType.KeyNames {
		if slices.Contains(columnNames, oldKeysType.KeyNames[i]) {
			oldKeysType.KeyNames[n] = oldKeysType.KeyNames[i]
			oldKeysType.KeyValues[n] = oldKeysType.KeyValues[i]
			n++
		}
	}
	oldKeysType.KeyNames = oldKeysType.KeyNames[:n]
	oldKeysType.KeyValues = oldKeysType.KeyValues[:n]
}

func allFieldsPresent(colNames []string, rawDocFields map[string]schema.Type, fields []string) bool {
	colSet := set.New[string](colNames...)
	for _, key := range fields {
		if _, ok := rawDocFields[key]; !ok && !colSet.Contains(key) {
			return false
		}
	}
	return true
}
