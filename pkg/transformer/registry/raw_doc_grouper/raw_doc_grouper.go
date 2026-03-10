package rawdocgrouper

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/exp/slices"
)

const (
	restField                    = "_rest"
	RawDocGrouperTransformerType = abstract.TransformerType("raw_doc_grouper")
)

var rawDocFields = map[string]schema.Type{
	etlUpdatedField: schema.TypeTimestamp,
	rawDataField:    schema.TypeAny,
}

func init() {
	transformer.Register[RawDocGrouperConfig](
		RawDocGrouperTransformerType,
		func(protoConfig RawDocGrouperConfig, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewRawDocGroupTransformer(protoConfig)
		},
	)
}

type RawDocGrouperConfig struct {
	Tables              filter.Tables `json:"tables"`
	Keys                []string      `json:"keys"`
	Fields              []string      `json:"fields"`
	ShouldUpdateOldKeys bool          `json:"shouldUpdateOldKeys,omitempty"`
}

type RawDocGroupTransformer struct {
	Tables        filter.Filter
	Keys          []string
	Fields        []string
	keySet        *set.Set[string]
	targetSchemas map[string]*abstract.TableSchema // key - schema.Hash, value - transformed schema
	schemasLock   sync.RWMutex

	// ShouldUpdateOldKeys variable to avoid changing the key for update
	// if it's guaranteed to change the additional key only together with the present
	ShouldUpdateOldKeys bool
	// additionalKeys used only when ShouldUpdateOldKeys == true, key - schema.Hash, value - slice of keys that were not key in the original schema
	additionalKeys map[string][]string
}

func (r *RawDocGroupTransformer) Type() abstract.TransformerType {
	return RawDocGrouperTransformerType
}

func (r *RawDocGroupTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)

	for _, changeItem := range input {
		// some system event
		if changeItem.TableSchema.Columns() == nil || len(changeItem.TableSchema.Columns()) == 0 {
			transformed = append(transformed, changeItem)
			continue
		}

		if !r.containsAllFields(changeItem.TableSchema.Columns().ColumnNames()) {
			errors = append(errors, abstract.TransformerError{
				Input: changeItem,
				Error: xerrors.Errorf("Data is not suitable for Transformer, required keys: %s, actual columns: %s",
					r.keySet.String(), changeItem.TableSchema.Columns().ColumnNames()),
			})
			continue
		}

		if changeItem.IsRowEvent() {
			cols, values := r.collectParsedData(changeItem.ColumnNames, changeItem.ColumnValues, changeItem.CommitTime)
			changeItem.ColumnNames = cols
			changeItem.ColumnValues = values
		}

		schemaHash, err := changeItem.TableSchema.Hash()
		resultSchema, _ := r.resultSchema(changeItem.TableSchema, schemaHash, err)
		if resultSchema == nil {
			errors = append(errors, abstract.TransformerError{
				Input: changeItem,
				Error: xerrors.Errorf("Could not determine result schema for change item, "+
					"perhaps schema hash was empty. Required keyset: %s", r.keySet.String()),
			})
		} else {
			changeItem = processUpdateItem(changeItem, r.additionalKeys[schemaHash], r.ShouldUpdateOldKeys)
			changeItem.SetTableSchema(resultSchema)
			transformed = append(transformed, changeItem)
		}
	}
	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      errors,
	}
}

func (r *RawDocGroupTransformer) containsAllKeys(colNames []string) bool {
	return allFieldsPresent(colNames, rawDocFields, r.Keys)
}

func (r *RawDocGroupTransformer) containsAllFields(colNames []string) bool {
	return allFieldsPresent(colNames, rawDocFields, append(r.Keys, r.Fields...))
}

func (r *RawDocGroupTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return filter.MatchAnyTableNameVariant(r.Tables, table) && schema != nil && r.containsAllFields(schema.Columns().ColumnNames())
}

func (r *RawDocGroupTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	schemaHash, err := original.Hash()

	return r.resultSchema(original, schemaHash, err)
}

func (r *RawDocGroupTransformer) resultSchema(original *abstract.TableSchema, schemaHash string, schemaHashError error) (*abstract.TableSchema, error) {
	if schemaHashError != nil || schemaHash == "" {
		logger.Log.Error("Can't get original schema hash!", log.Error(schemaHashError))
		return nil, nil
	}

	r.schemasLock.Lock()
	defer r.schemasLock.Unlock()
	tableTargetSchema := r.targetSchemas[schemaHash]
	if tableTargetSchema != nil {
		return tableTargetSchema, nil
	}

	colNameToIdx := abstract.MakeMapColNameToIndex(original.Columns())

	keys := CollectFieldsForTransformer(r.Keys, original.Columns(), true, colNameToIdx, rawDocFields)
	fields := CollectFieldsForTransformer(r.Fields, original.Columns(), false, colNameToIdx, rawDocFields)
	if r.ShouldUpdateOldKeys {
		r.additionalKeys[schemaHash] = CollectAdditionalKeysForTransformer(r.Keys, abstract.KeyNames(original.Columns()))
	}
	tableTargetSchema = abstract.NewTableSchema(append(keys, fields...))
	r.targetSchemas[schemaHash] = tableTargetSchema
	return tableTargetSchema, nil
}

func (r *RawDocGroupTransformer) Description() string {
	return fmt.Sprintf("Return item as primary keys %s and json, containing the rest", strings.Join(r.Keys, ", "))
}

func (r *RawDocGroupTransformer) collectParsedData(colNames []string, colValues []interface{}, atime uint64) ([]string, []interface{}) {
	newCols := make([]string, 0, r.keySet.Len()+2)
	newValues := make([]interface{}, 0, r.keySet.Len()+2)
	docData := make(map[string]interface{}, len(colNames))

	for idx, colName := range colNames {
		colValue := colValues[idx]

		if colName == restField {
			restMap, ok := colValue.(map[string]interface{})

			if ok {
				for innerKey, innerVal := range restMap {
					docData[innerKey] = innerVal
				}
			} else {
				docData[colName] = colValue
			}
		} else {
			docData[colName] = colValue
		}

		if r.keySet.Contains(colName) || slices.Contains(r.Fields, colName) {
			newCols = append(newCols, colName)
			newValues = append(newValues, colValue)
		}
	}

	newCols = append(newCols, etlUpdatedField)
	newValues = append(newValues, time.Unix(0, int64(atime)))

	newCols = append(newCols, rawDataField)
	newValues = append(newValues, docData)

	return newCols, newValues
}

func NewRawDocGroupTransformer(config RawDocGrouperConfig) (*RawDocGroupTransformer, error) {
	keys := config.Keys
	var fields []string
	if len(config.Fields) > 0 {
		fields = config.Fields
	}

	for _, name := range []string{etlUpdatedField, rawDataField} {
		if !slices.Contains(keys, name) && !slices.Contains(fields, name) {
			fields = append(fields, name)
		}
	}

	keySet := set.New[string](keys...)
	if len(keys) != keySet.Len() {
		return nil, xerrors.Errorf("Can't use same keys column names twice: %s", strings.Join(keys, ", "))
	}

	for _, key := range keys {
		for _, nonKey := range fields {
			if key == nonKey {
				return nil, xerrors.Errorf("Can't use same column as key and non-key : %s", key)
			}
		}
	}

	tables, err := filter.NewFilter(config.Tables.IncludeTables, config.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("unable to init table filter: %w", err)
	}
	return &RawDocGroupTransformer{
		Keys:                keys,
		Fields:              fields,
		keySet:              keySet,
		Tables:              tables,
		ShouldUpdateOldKeys: config.ShouldUpdateOldKeys,
		additionalKeys:      make(map[string][]string),
		targetSchemas:       make(map[string]*abstract.TableSchema),
		schemasLock:         sync.RWMutex{},
	}, nil

}
