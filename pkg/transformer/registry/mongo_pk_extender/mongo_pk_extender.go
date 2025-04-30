package mongo_pk_extender

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.mongodb.org/mongo-driver/bson"
	"go.ytsaurus.tech/library/go/core/log"
)

const Type = abstract.TransformerType("mongo_pk_extender")

const ID = "id"

type Config struct {
	Tables          filter.Tables `json:"tables"`
	Expand          bool
	ExtraFieldName  string
	ExtraFieldValue string
}

type MongoPKExtenderTransformer struct {
	tables          filter.Filter
	expand          bool
	extraFieldName  string
	extraFieldValue interface{}
	logger          log.Logger
}

func init() {
	transformer.Register[Config](
		Type,
		func(protoConfig Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewMongoPKExtenderTransformer(protoConfig, lgr)
		},
	)
}

func parseValue(value string) interface{} {
	trimmed := strings.TrimSpace(value)
	if b, err := strconv.ParseBool(trimmed); err == nil {
		return b
	}

	if i, err := strconv.ParseInt(trimmed, 10, 0); err == nil {
		return i
	}

	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return f
	}

	return value
}

func NewMongoPKExtenderTransformer(config Config, lgr log.Logger) (*MongoPKExtenderTransformer, error) {
	tables, err := filter.NewFilter(config.Tables.IncludeTables, config.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("Unable to init table filter: %w", err)
	}
	extraFieldValue := parseValue(config.ExtraFieldValue)
	return &MongoPKExtenderTransformer{tables: tables, expand: config.Expand, extraFieldName: config.ExtraFieldName, extraFieldValue: extraFieldValue, logger: lgr}, nil
}

func (t *MongoPKExtenderTransformer) Type() abstract.TransformerType {
	return Type
}

func (t *MongoPKExtenderTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)

	for _, item := range input {
		if item.IsRowEvent() {
			if processed, err := t.processItem(item); err == nil {
				transformed = append(transformed, processed)
			}
		} else {
			transformed = append(transformed, item)
		}
	}

	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      errors,
	}
}

func (t *MongoPKExtenderTransformer) processItem(item abstract.ChangeItem) (abstract.ChangeItem, error) {
	fastColumns := item.TableSchema.FastColumns()
	for i, columnName := range item.ColumnNames {
		if item.ColumnValues[i] == nil {
			continue
		}

		if colScheme, ok := fastColumns[abstract.ColumnName(columnName)]; ok {
			if !colScheme.IsKey() {
				continue
			}
		}

		if transformed, err := t.processValue(item.ColumnValues[i]); err == nil {
			item.ColumnValues[i] = transformed
		} else {
			return item, err
		}
	}

	item.OldKeys = t.createOldKeys(item)
	return item, nil
}

func equals(x, y interface{}) bool {
	xv := reflect.ValueOf(x)
	yv := reflect.ValueOf(y)
	if yv.Type().ConvertibleTo(xv.Type()) {
		return xv.Interface() == yv.Convert(xv.Type()).Interface()
	} else {
		return false
	}
}

func (t *MongoPKExtenderTransformer) processValue(value interface{}) (interface{}, error) {
	if t.expand {
		return bson.D{{Key: t.extraFieldName, Value: t.extraFieldValue}, {Key: ID, Value: value}}, nil
	} else {
		doc, ok := value.(bson.D)
		if ok {
			var extraFieldFound bool
			var idValue interface{}
			for _, elem := range doc {
				if elem.Key == t.extraFieldName && equals(elem.Value, t.extraFieldValue) {
					extraFieldFound = true
				}
				if elem.Key == ID {
					idValue = elem.Value
				}
			}
			if extraFieldFound && idValue != nil {
				return idValue, nil
			}
		}
		return nil, xerrors.Errorf("Not supported %v", value)
	}
}

func (t *MongoPKExtenderTransformer) createOldKeys(item abstract.ChangeItem) abstract.OldKeysType {
	switch {
	case item.Kind == abstract.InsertKind:
		keyNames := make([]string, 0, 1)
		keyTypes := make([]string, 0, 1)
		keyValues := make([]interface{}, 0, 1)
		fastColumns := item.TableSchema.FastColumns()
		for i, columnName := range item.ColumnNames {
			if colScheme, ok := fastColumns[abstract.ColumnName(columnName)]; ok {
				if colScheme.IsKey() {
					keyTypes = append(keyTypes, colScheme.DataType)
					keyNames = append(keyNames, colScheme.ColumnName)
					keyValues = append(keyValues, item.ColumnValues[i])
					break
				}
			}
		}
		return abstract.OldKeysType{
			KeyNames:  keyNames,
			KeyTypes:  keyTypes,
			KeyValues: keyValues,
		}
	case item.Kind == abstract.DeleteKind || item.Kind == abstract.UpdateKind:
		keyValues := make([]interface{}, len(item.OldKeys.KeyValues))
		for i, keyValue := range item.OldKeys.KeyValues {
			if transformed, err := t.processValue(keyValue); err == nil {
				keyValues[i] = transformed
			} else {
				keyValues[i] = keyValue
			}
		}
		return abstract.OldKeysType{
			KeyNames:  item.OldKeys.KeyNames,
			KeyTypes:  item.OldKeys.KeyTypes,
			KeyValues: keyValues,
		}
	default:
		return item.OldKeys
	}
}

func (t *MongoPKExtenderTransformer) Suitable(table abstract.TableID, _ *abstract.TableSchema) bool {
	return t.tables.Match(table.Name)
}

func (t *MongoPKExtenderTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *MongoPKExtenderTransformer) Description() string {
	return "Extend Mongo PK with extra value"
}
