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

const RawCdcDocGrouperTransformerType = abstract.TransformerType("raw_cdc_doc_grouper")

var rawCdcDocFields = map[string]schema.Type{
	etlUpdatedField: schema.TypeTimestamp,
	deletedField:    schema.TypeBoolean,
	rawDataField:    schema.TypeAny,
}

func init() {
	transformer.Register[RawCDCDocGrouperConfig](
		RawCdcDocGrouperTransformerType,
		func(protoConfig RawCDCDocGrouperConfig, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewCdcHistoryGroupTransformer(protoConfig)
		},
	)
}

type RawCDCDocGrouperConfig struct {
	Tables filter.Tables `json:"tables"`
	Keys   []string      `json:"keys"`
	Fields []string      `json:"fields"`
}

type CdcHistoryGroupTransformer struct {
	Tables        filter.Filter
	Keys          []string
	Fields        []string
	keySet        *set.Set[string]
	targetSchemas map[string]*abstract.TableSchema
	schemasLock   sync.RWMutex
}

func (r *CdcHistoryGroupTransformer) Type() abstract.TransformerType {
	return RawCdcDocGrouperTransformerType
}

func (r *CdcHistoryGroupTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)

	for _, changeItem := range input {
		// some system event just passing through
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
			cols, values := r.collectParsedData(changeItem)
			changeItem.ColumnNames = cols
			changeItem.ColumnValues = values
			//only insert new lines for cdc history
			changeItem.Kind = abstract.InsertKind
		}

		resultSchema, _ := r.ResultSchema(changeItem.TableSchema)
		if resultSchema == nil {
			errors = append(errors, abstract.TransformerError{
				Input: changeItem,
				Error: xerrors.Errorf("Could not determine result schema for change item, "+
					"perhaps schema hash was empty. Required keyset: %s", r.keySet.String()),
			})
		} else {
			changeItem.SetTableSchema(resultSchema)
			transformed = append(transformed, changeItem)
		}
	}
	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      errors,
	}
}

func (r *CdcHistoryGroupTransformer) containsAllKeys(colNames []string) bool {
	return allFieldsPresent(colNames, rawCdcDocFields, r.Keys)
}

func (r *CdcHistoryGroupTransformer) containsAllFields(colNames []string) bool {
	return allFieldsPresent(colNames, rawCdcDocFields, append(r.Keys, r.Fields...))
}

func (r *CdcHistoryGroupTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return filter.MatchAnyTableNameVariant(r.Tables, table) && schema != nil && r.containsAllFields(schema.Columns().ColumnNames())
}

func (r *CdcHistoryGroupTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	schemaHash, err := original.Hash()
	if err != nil || schemaHash == "" {
		logger.Log.Error("Can't get original schema hash!", log.Error(err))
		return nil, nil
	}
	r.schemasLock.Lock()
	defer r.schemasLock.Unlock()
	tableTargetSchema := r.targetSchemas[schemaHash]
	if tableTargetSchema != nil {
		return tableTargetSchema, nil
	}
	// schema consists of: primary keys + non-keys, including doc field with data + system timestamp + deleted flg
	colNameToIdx := abstract.MakeMapColNameToIndex(original.Columns())

	keys := CollectFieldsForTransformer(r.Keys, original.Columns(), true, colNameToIdx, rawCdcDocFields)
	fields := CollectFieldsForTransformer(r.Fields, original.Columns(), false, colNameToIdx, rawCdcDocFields)

	tableTargetSchema = abstract.NewTableSchema(append(keys, fields...))
	r.targetSchemas[schemaHash] = tableTargetSchema
	return tableTargetSchema, nil
}

func (r *CdcHistoryGroupTransformer) Description() string {
	return fmt.Sprintf("Return item as primary keys %s, system fields %s and json, containing the rest",
		strings.Join(r.Keys, ", "), strings.Join(r.Fields, ", "))
}

func (r *CdcHistoryGroupTransformer) collectParsedData(changeItem abstract.ChangeItem) ([]string, []interface{}) {

	newCols := make([]string, 0, len(r.Keys)+len(r.Fields))
	newValues := make([]interface{}, 0, len(r.Keys)+len(r.Fields))
	docData := make(map[string]interface{}, len(changeItem.ColumnNames))

	var columnNames []string
	var columnValues []interface{}

	if changeItem.Kind == abstract.DeleteKind {
		old := changeItem.OldKeys
		columnNames = old.KeyNames
		columnValues = old.KeyValues
	} else {
		columnNames = changeItem.ColumnNames
		columnValues = changeItem.ColumnValues
	}

	// Firstly adding data from original columns and collecting doc
	for idx, colName := range columnNames {
		colValue := columnValues[idx]
		docData[colName] = colValue
		if r.keySet.Contains(colName) || slices.Contains(r.Fields, colName) {
			newCols = append(newCols, colName)
			newValues = append(newValues, colValue)
		}
	}

	// Adding key columns that were absent in original changeItem
	for _, colSchema := range changeItem.TableSchema.Columns() {
		if !r.keySet.Contains(colSchema.ColumnName) {
			continue
		}
		if _, ok := docData[colSchema.ColumnName]; !ok {
			newCols = append(newCols, colSchema.ColumnName)
			newValues = append(newValues, nil) // that is how it used to work
		}
	}

	// Adding system columns
	newCols = append(newCols, etlUpdatedField)
	newValues = append(newValues, time.Unix(0, int64(changeItem.CommitTime)))

	newCols = append(newCols, deletedField)
	newValues = append(newValues, changeItem.Kind == abstract.DeleteKind)

	newCols = append(newCols, rawDataField)
	newValues = append(newValues, docData)

	return newCols, newValues
}

func NewCdcHistoryGroupTransformer(config RawCDCDocGrouperConfig) (*CdcHistoryGroupTransformer, error) {

	keys := config.Keys
	var fields []string
	if len(config.Fields) > 0 {
		fields = config.Fields
	}

	for _, name := range []string{etlUpdatedField, deletedField, rawDataField} {
		if !slices.Contains(keys, name) && !slices.Contains(fields, name) {
			if name == etlUpdatedField {
				//by default to the beginning
				keys = append([]string{etlUpdatedField}, keys...)
			} else {
				fields = append(fields, name)
			}
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

	return &CdcHistoryGroupTransformer{
		Keys:          keys,
		keySet:        keySet,
		Tables:        tables,
		Fields:        fields,
		targetSchemas: make(map[string]*abstract.TableSchema),
		schemasLock:   sync.RWMutex{},
	}, nil

}
