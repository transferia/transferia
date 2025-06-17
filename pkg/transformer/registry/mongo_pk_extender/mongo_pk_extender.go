//go:build !disable_mongo_provider

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

type SchemaDiscriminator struct {
	Schema string
	Value  string
}

type Config struct {
	Tables              filter.Tables `json:"tables"`
	Expand              bool
	DiscriminatorField  string
	DiscriminatorValues []SchemaDiscriminator
}

type MongoPKExtenderTransformer struct {
	tables              filter.Filter
	expand              bool
	discriminatorField  string
	discriminatorValues map[string]interface{}
	logger              log.Logger
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
	discriminatorValuesMap := make(map[string]interface{}, len(config.DiscriminatorValues))
	for _, item := range config.DiscriminatorValues {
		discriminatorValuesMap[item.Schema] = parseValue(item.Value)
	}
	return &MongoPKExtenderTransformer{tables: tables, expand: config.Expand, discriminatorField: config.DiscriminatorField, discriminatorValues: discriminatorValuesMap, logger: lgr}, nil
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

		if t.expand {
			item.ColumnValues[i] = t.expandValue(item.ColumnValues[i], item)
		} else {
			if idValue, schema, err := t.collapseValue(item.ColumnValues[i]); err == nil {
				item.ColumnValues[i] = idValue
				item.Schema = schema
			} else {
				return item, err
			}
		}
	}

	oldKeys, schema := t.createOldKeys(item)
	item.OldKeys = oldKeys
	item.Schema = schema
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

func (t *MongoPKExtenderTransformer) expandValue(value interface{}, item abstract.ChangeItem) interface{} {
	discriminator := t.discriminatorValues[item.Schema]
	return bson.D{{Key: t.discriminatorField, Value: discriminator}, {Key: ID, Value: value}}
}

func (t *MongoPKExtenderTransformer) collapseValue(value interface{}) (interface{}, string, error) {
	doc, ok := value.(bson.D)
	if ok {
		var idValue interface{}
		var discriminatorValue interface{}
		for _, elem := range doc {
			if elem.Key == t.discriminatorField {
				discriminatorValue = elem.Value
			}
			if elem.Key == ID {
				idValue = elem.Value
			}
		}
		if idValue != nil && discriminatorValue != nil {
			for k, v := range t.discriminatorValues {
				if equals(discriminatorValue, v) {
					return idValue, k, nil
				}
			}
		}
	}
	return nil, "", xerrors.Errorf("Not supported %v", value)
}

func (t *MongoPKExtenderTransformer) createOldKeys(item abstract.ChangeItem) (abstract.OldKeysType, string) {
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
		}, item.Schema
	case item.Kind == abstract.DeleteKind || item.Kind == abstract.UpdateKind:
		schema := item.Schema
		keyValues := make([]interface{}, len(item.OldKeys.KeyValues))
		for i, keyValue := range item.OldKeys.KeyValues {
			if t.expand {
				keyValues[i] = t.expandValue(keyValue, item)
			} else {
				if transformed, newSchema, err := t.collapseValue(keyValue); err == nil {
					keyValues[i] = transformed
					schema = newSchema
				} else {
					keyValues[i] = keyValue
				}
			}
		}
		return abstract.OldKeysType{
			KeyNames:  item.OldKeys.KeyNames,
			KeyTypes:  item.OldKeys.KeyTypes,
			KeyValues: keyValues,
		}, schema
	default:
		return item.OldKeys, item.Schema
	}
}

func (t *MongoPKExtenderTransformer) Suitable(table abstract.TableID, _ *abstract.TableSchema) bool {
	return t.tables.Match(table.Name)
}

func (t *MongoPKExtenderTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *MongoPKExtenderTransformer) Description() string {
	return "Extend Mongo PK with discriminator value"
}
