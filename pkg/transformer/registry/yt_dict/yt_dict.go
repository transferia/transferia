//go:build !disable_yt_provider

package ytdict

import (
	"fmt"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/xerrors"
)

const (
	Type = abstract.TransformerType("yt_dict_transformer")
)

func init() {
	transformer.Register(
		Type,
		func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewYtDictTransformer(cfg, lgr)
		},
	)
}

type Config struct {
	Tables filter.Tables `json:"tables"`
}

type YtDictTransformer struct {
	Tables filter.Filter
	Logger log.Logger
}

func NewYtDictTransformer(cfg Config, lgr log.Logger) (*YtDictTransformer, error) {
	tables, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("unable to init table filter: %w", err)
	}
	return &YtDictTransformer{Tables: tables, Logger: lgr}, nil
}

func (t *YtDictTransformer) Type() abstract.TransformerType {
	return Type
}

func (t *YtDictTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return filter.MatchAnyTableNameVariant(t.Tables, table)
}

func (t *YtDictTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *YtDictTransformer) Description() string {
	return "Transformer for converting original yt composite types to human-friendly."
}

func (t *YtDictTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)

	for _, item := range input {
		isNameMatching := filter.MatchAnyTableNameVariant(t.Tables, item.TableID())
		if !isNameMatching || abstract.IsSystemTable(item.TableID().Name) {
			transformed = append(transformed, item)
			continue
		}

		transformedItem, err := t.processChangeItem(item)
		if err != nil {
			errors = append(errors, abstract.TransformerError{Input: item, Error: abstract.NewFatalError(err)})
			continue
		}
		transformed = append(transformed, transformedItem)
	}
	return abstract.TransformerResult{Transformed: transformed, Errors: errors}
}

func (t *YtDictTransformer) processChangeItem(item abstract.ChangeItem) (abstract.ChangeItem, error) {
	columns := item.TableSchema.Columns()
	colNameToIndex := abstract.MakeMapColNameToIndex(columns)
	for i, columnName := range item.ColumnNames {
		if columnName == "dict" {
			fmt.Println("ok")
		}
		column := columns[colNameToIndex[columnName]]
		ytType, found := column.Properties[table.YtOriginalTypePropertyKey]
		if !found || item.ColumnValues[i] == nil {
			continue
		}
		complexType, ok := ytType.(schema.ComplexType)
		if !ok {
			return item, xerrors.Errorf("unable to get complex type for column '%s', got '%T'", columnName, ytType)
		}
		values, err := t.processAnything(item.ColumnValues[i], complexType)
		if err != nil {
			return item, xerrors.Errorf("unable to process values of column '%s': %w", columnName, err)
		}
		item.ColumnValues[i] = values
	}
	return item, nil
}

func (t *YtDictTransformer) processAnything(val any, complexType schema.ComplexType) (any, error) {
	switch valSchema := complexType.(type) {
	case schema.Type, schema.Decimal:
		return val, nil

	case schema.Optional:
		res, err := t.processAnything(val, valSchema.Item)
		if err != nil {
			return nil, xerrors.Errorf("unable to process optional: %w", err)
		}
		return res, nil

	case schema.Tagged:
		res, err := t.processAnything(val, valSchema.Item)
		if err != nil {
			return nil, xerrors.Errorf("unable to process tagged: %w", err)
		}
		return res, nil

	case schema.List:
		res, err := t.processList(val, valSchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to process list: %w", err)
		}
		return res, nil

	case schema.Tuple:
		res, err := t.processTuple(val, valSchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to process tuple: %w", err)
		}
		return res, nil

	case schema.Struct:
		res, err := t.processStruct(val, valSchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to process struct: %w", err)
		}
		return res, nil

	case schema.Variant:
		res, err := t.processVariant(val, valSchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to process variant: %w", err)
		}
		return res, nil

	case schema.Dict:
		res, err := t.processDict(val, valSchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to process dict: %w", err)
		}
		return res, nil
	}

	return nil, xerrors.Errorf("got value with unexpected type '%T'", val)
}

// processList iterates over list and applies transformation to every element of it.
func (t *YtDictTransformer) processList(val any, valSchema schema.List) (any, error) {
	if _, ok := valSchema.Item.(schema.Type); ok {
		return val, nil // List of simple types, it is already stored as []type.
	}
	list, ok := val.([]any)
	if !ok {
		return nil, xerrors.Errorf("unable to cast value to []any, got '%T'", val)
	}
	for i := range list {
		newItem, err := t.processAnything(list[i], valSchema.Item)
		if err != nil {
			return nil, xerrors.Errorf("unable to process list's element: %w", err)
		}
		list[i] = newItem
	}
	return list, nil
}

// processList iterates over tuple and applies transformation to every element of it.
func (t *YtDictTransformer) processTuple(val any, valSchema schema.Tuple) (any, error) {
	tuple, ok := val.([]any)
	if !ok {
		return nil, xerrors.Errorf("unable to cast val to []any, got '%T'", val)
	}
	for i, element := range valSchema.Elements {
		newElement, err := t.processAnything(tuple[i], element.Type)
		if err != nil {
			return nil, xerrors.Errorf("unable to process tuple's element: %w", err)
		}
		tuple[i] = newElement
	}
	return tuple, nil
}

// processStruct iterates over struct's fields and applies transformation to every element of it.
func (t *YtDictTransformer) processStruct(val any, valSchema schema.Struct) (any, error) {
	structure, ok := val.(map[string]any)
	if !ok {
		return nil, xerrors.Errorf("unable to cast value to map[string]any, got '%T'", val)
	}
	for _, member := range valSchema.Members {
		newMember, err := t.processAnything(structure[member.Name], member.Type)
		if err != nil {
			return nil, xerrors.Errorf("unable to process structure's member '%s': %w", member.Name, err)
		}
		structure[member.Name] = newMember
	}
	return structure, nil
}

// processDict iterates over dict's key-value pairs and applies transformation to each of it.
func (t *YtDictTransformer) processDict(val any, valSchema schema.Dict) (any, error) {
	// YT go SDK returns dict as []any ~ [[key1, value1], [key2, value2]],
	// transformer change it to map[keyType]any ~ {key1: value1, key2: value2}.
	dict, ok := val.([]any)
	if !ok {
		return nil, xerrors.Errorf("unable to cast value to []any, got '%T'", val)
	}
	var result any
	for i := range dict {
		element, ok := dict[i].([]any)
		if !ok {
			return nil, xerrors.Errorf("unable to cast dict's element to []any, got '%T'", dict[i])
		}
		key, err := t.processAnything(element[0], valSchema.Key)
		if err != nil {
			return nil, xerrors.Errorf("unable to process dict's key: %w", err)
		}
		value, err := t.processAnything(element[1], valSchema.Value)
		if err != nil {
			return nil, xerrors.Errorf("unable to process dict's value: %w", err)
		}
		result, err = upsertToDict(result, key, value, valSchema.Key)
		if err != nil {
			return nil, xerrors.Errorf("unable to upsert to dict: %w", err)
		}
	}
	return result, nil
}

// processVariant returns schema of  and applies transformation to every element of it.
func (t *YtDictTransformer) processVariant(val any, valSchema schema.Variant) (any, error) {
	variant, ok := val.([]any)
	if !ok {
		return nil, xerrors.Errorf("unable to cast variant value to []any, got '%T'", val)
	}
	key, value := variant[0], variant[1]
	// Variant allows user to specify N schemas for column and select one to use for every row separately.
	// Here, in `valSchema` we have N schemas (for every variant). But `val` contains only value of type,
	// selected by user and its key. We need to proccess `val` with t.processAnything, but provide to it
	// schema of only selected type.
	selectedSchema, err := selectedVariantSchema(key, valSchema)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract selected variant schema: %w", err)
	}
	value, err = t.processAnything(value, selectedSchema)
	if err != nil {
		return nil, xerrors.Errorf("unable to process unwrapped variant value with key '%v': %w", variant[0], err)
	}
	return []any{key, value}, nil
}

// selectedVariantSchema extracts ComplexType of selected by user variant.
func selectedVariantSchema(key any, valSchema schema.Variant) (schema.ComplexType, error) {
	switch key := key.(type) {
	case int64: // Unnamed variant.
		if valSchema.Elements == nil {
			return nil, xerrors.New("expected not-nil variant's elements")
		}
		return valSchema.Elements[key].Type, nil

	case string: // Named variant.
		if valSchema.Members == nil {
			return nil, xerrors.New("expected not-nil variant's members")
		}
		for _, member := range valSchema.Members {
			if member.Name == key {
				return member.Type, nil
			}
		}
		return nil, xerrors.Errorf("unable to find variant member with key '%s'", key)
	}
	return nil, xerrors.Errorf("got key with unexpected type '%T'", key)
}
