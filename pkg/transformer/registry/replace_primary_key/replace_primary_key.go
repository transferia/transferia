package replaceprimarykey

import (
	"fmt"
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/transformer"
	"github.com/transferria/transferria/pkg/transformer/registry/filter"
	"github.com/transferria/transferria/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

const Type = abstract.TransformerType("replace_primary_key")

func init() {
	transformer.Register[Config](Type, func(cfg Config, _ log.Logger, _ abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		return NewReplacePrimaryKeyTransformer(cfg)
	})
}

type Config struct {
	Keys   []string      `json:"keys"`
	Tables filter.Tables `json:"tables"`
}

type ReplacePrimaryKeyTransformer struct {
	Tables filter.Filter
	Keys   []string
	keySet *set.Set[string]
}

func (r *ReplacePrimaryKeyTransformer) Type() abstract.TransformerType {
	return Type
}

func (r *ReplacePrimaryKeyTransformer) containsAllKeys(colNames []string) bool {
	containsKeysCounter := 0
	for _, name := range colNames {
		if r.isNewKey(name) {
			containsKeysCounter++
		}
	}
	return r.keySet.Len() == containsKeysCounter
}

func (r *ReplacePrimaryKeyTransformer) isNewKey(colName string) bool {
	return r.keySet.Contains(colName)
}

func (r *ReplacePrimaryKeyTransformer) createOldKeys(item abstract.ChangeItem) abstract.OldKeysType {
	keyNames := make([]string, 0, len(r.Keys))
	keyTypes := make([]string, 0, len(r.Keys))
	for _, key := range r.Keys {
		for _, col := range item.TableSchema.Columns() {
			if key != col.ColumnName {
				continue
			}
			keyTypes = append(keyTypes, col.DataType)
			keyNames = append(keyNames, col.ColumnName)
			break
		}
	}
	keyValues := make([]interface{}, len(keyNames))
	for i, name := range keyNames {
		for j, colName := range item.ColumnNames {
			if name == colName {
				keyValues[i] = item.ColumnValues[j]
				break
			}
		}
	}
	return abstract.OldKeysType{
		KeyNames:  keyNames,
		KeyTypes:  keyTypes,
		KeyValues: keyValues,
	}
}

func (r *ReplacePrimaryKeyTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	for _, changeItem := range input {
		if r.Suitable(changeItem.TableID(), changeItem.TableSchema) {
			res, _ := r.ResultSchema(changeItem.TableSchema)
			changeItem.SetTableSchema(res)
		}
		if changeItem.Kind == abstract.UpdateKind {
			changeItem.OldKeys = r.createOldKeys(changeItem)
		}
		transformed = append(transformed, changeItem)
	}
	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      nil,
	}
}

func (r *ReplacePrimaryKeyTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return filter.MatchAnyTableNameVariant(r.Tables, table) && r.containsAllKeys(schema.Columns().ColumnNames())
}

func (r *ReplacePrimaryKeyTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	result := make([]abstract.ColSchema, 0, len(original.Columns()))
	if len(r.Keys) == 1 {
		for _, colSchema := range original.Columns() {
			colSchema.PrimaryKey = r.isNewKey(colSchema.ColumnName)
			result = append(result, colSchema)
		}
	} else {
		// difficult case - composite primary key, maybe need reorder columns
		colNameToIdx := abstract.MakeMapColNameToIndex(original.Columns())
		for _, key := range r.Keys {
			colSchema := original.Columns()[colNameToIdx[key]]
			colSchema.PrimaryKey = true
			result = append(result, colSchema)
		}
		for _, colSchema := range original.Columns() {
			if !r.isNewKey(colSchema.ColumnName) {
				colSchema.PrimaryKey = false
				result = append(result, colSchema)
			}
		}
	}

	return abstract.NewTableSchema(result), nil
}

func (r *ReplacePrimaryKeyTransformer) Description() string {
	return fmt.Sprintf("Replace primary keys to: %s ", strings.Join(r.Keys, ", "))
}

func NewReplacePrimaryKeyTransformer(cfg Config) (*ReplacePrimaryKeyTransformer, error) {
	keySet := set.New[string](cfg.Keys...)
	if len(cfg.Keys) != keySet.Len() {
		return nil, xerrors.Errorf("Can't use same keys column names twice: %s", strings.Join(cfg.Keys, ", "))
	}
	tbls, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("unable to create tables filter: %w", err)
	}
	return &ReplacePrimaryKeyTransformer{
		Tables: tbls,
		Keys:   cfg.Keys,
		keySet: keySet,
	}, nil
}
