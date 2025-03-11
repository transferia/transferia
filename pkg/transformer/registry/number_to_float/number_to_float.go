package numbertofloat

import (
	"container/list"
	"encoding/json"

	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/transformer"
	"github.com/transferria/transferria/pkg/transformer/registry/filter"
	"github.com/transferria/transferria/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	yts "go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/xerrors"
)

const (
	Type = abstract.TransformerType("number_to_float_transformer")
)

var (
	supportedKinds = set.New(abstract.InsertKind, abstract.UpdateKind)
)

func init() {
	transformer.Register[Config](
		Type,
		func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewNumberToFloatTransformer(cfg, lgr)
		},
	)
}

type Config struct {
	Tables filter.Tables `json:"tables"`
}

type NumberToFloatTransformer struct {
	Tables filter.Filter
	Logger log.Logger
}

func NewNumberToFloatTransformer(cfg Config, lgr log.Logger) (*NumberToFloatTransformer, error) {
	tables, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("Unable to init table filter: %w", err)
	}
	return &NumberToFloatTransformer{Tables: tables, Logger: lgr}, nil
}

func (t *NumberToFloatTransformer) Type() abstract.TransformerType {
	return Type
}

func (t *NumberToFloatTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)
	for _, item := range input {
		isNameMatching := filter.MatchAnyTableNameVariant(t.Tables, item.TableID())
		if !isNameMatching || abstract.IsSystemTable(item.TableID().Name) || !supportedKinds.Contains(item.Kind) {
			transformed = append(transformed, item)
			continue
		}
		transformed = append(transformed, t.processItem(item))
	}

	return abstract.TransformerResult{Transformed: transformed, Errors: errors}
}

func (t *NumberToFloatTransformer) processItem(item abstract.ChangeItem) abstract.ChangeItem {
	schemaColumns := item.TableSchema.Columns()
	schemaMapping := abstract.MakeMapColNameToIndex(schemaColumns)
	for i, columnName := range item.ColumnNames {
		if schemaColumns[schemaMapping[columnName]].DataType == yts.TypeAny.String() {
			item.ColumnValues[i] = t.processColumnValue(item.ColumnValues[i])
		}
	}
	return item
}

// processColumnValue will change json.Number to float64 in-place for maps and slices of any nesting degree.
// But you should use returned val in case if it is only json.Number provided and cannot be changed in-place.
func (t *NumberToFloatTransformer) processColumnValue(val interface{}) interface{} {
	if asNumber, ok := val.(json.Number); ok {
		asFloat64, err := asNumber.Float64()
		if err == nil {
			return asFloat64
		}
	}
	queue := list.New()
	queue.PushBack(val)
	for queue.Len() > 0 {
		top := queue.Front().Value
		queue.Remove(queue.Front())
		t.replaceNumbers(queue, top)
	}
	return val
}

func (t *NumberToFloatTransformer) replaceNumbers(queue *list.List, val interface{}) {
	switch typed := val.(type) {
	case map[string]interface{}:
		for k, v := range typed {
			switch typedV := v.(type) {
			case map[string]interface{}, []interface{}:
				queue.PushBack(typed[k]) // we can pushback slice because len(typed[k]) won't change
			case json.Number:
				if asFloat64, err := typedV.Float64(); err == nil {
					typed[k] = asFloat64
				}
			}
		}

	case []interface{}:
		for i, v := range typed {
			switch typedV := v.(type) {
			case map[string]interface{}, []interface{}:
				queue.PushBack(typed[i]) // we can pushback slice because len(typed[k]) won't change
			case json.Number:
				if asFloat64, err := typedV.Float64(); err == nil {
					typed[i] = asFloat64
				}
			}
		}
	}
}

func (t *NumberToFloatTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return filter.MatchAnyTableNameVariant(t.Tables, table)
}

func (t *NumberToFloatTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *NumberToFloatTransformer) Description() string {
	return "Transformer for converting json.Number to Golang's float64."
}
