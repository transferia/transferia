package engine

import (
	"bytes"
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/util/set"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type JSONProperties struct {
	Type       string                     `json:"type"`
	OneOf      []*JSONProperties          `json:"oneOf"`
	Properties map[string]*JSONProperties `json:"properties"`
	Title      string                     `json:"title"`
	Required   []string                   `json:"required"`
}

func processPayload(schemaName, tableName string, jsonSchema *JSONProperties, payload []byte, isGenerateUpdates bool) (*abstract.TableSchema, []string, []interface{}, error) {
	var rows []abstract.ColSchema
	var names []string
	var values []interface{}

	if jsonSchema == nil {
		return nil, nil, nil, xerrors.Errorf("json schema can't be empty")
	}
	if jsonSchema.Type != "object" {
		return nil, nil, nil, xerrors.Errorf("json schema type must be 'object'")
	}

	requiredSet := set.New(jsonSchema.Required...)

	schemaRowNames := util.MapKeysInOrder(jsonSchema.Properties)
	for _, name := range schemaRowNames {
		rows = append(rows, jsonPropertyToJSONSchemaRow(schemaName, tableName, name, jsonSchema.Properties[name], requiredSet.Contains(name)))
	}
	var dataChanges map[string]interface{}
	if err := jsonx.NewDefaultDecoder(bytes.NewReader(payload)).Decode(&dataChanges); err != nil {
		return nil, nil, nil, xerrors.Errorf("Can't unmarshal data changes from message: %w", err)
	}
	for _, row := range rows {
		if value, ok := dataChanges[row.ColumnName]; ok {
			// value is present
			names = append(names, row.ColumnName)
			converted, err := convertTypes(value, ytschema.Type(row.DataType), !row.Required)
			if err != nil {
				return nil, nil, nil, xerrors.Errorf("Can't convert %q value %v (type %T) to type %q: %w", row.ColumnName, value, value, ytschema.Type(row.DataType), err)
			}
			values = append(values, converted)
		} else if row.Required {
			// value is absent, but MUST BE
			return nil, nil, nil, xerrors.Errorf("Field %q is required, but not found in payload %q", row.ColumnName, string(payload))
		} else {
			// value is absent, and it's ok
			if !isGenerateUpdates {
				// for inserts - all values from TableSchema should be in ColumnNames/ColumnValues
				names = append(names, row.ColumnName)
				values = append(values, nil)
			}
		}
	}

	return abstract.NewTableSchema(rows), names, values, nil
}

func jsonPropertyToJSONSchemaRow(schemaName, tableName, name string, property *JSONProperties, inIsRequired bool) abstract.ColSchema {
	colType := jsonSchemaTypes[jsonType(property.Type)].String()
	isRequired := inIsRequired
	if property.OneOf != nil {
		for _, currProperty := range property.OneOf {
			if currProperty.Type == JSONTypeNull.String() {
				isRequired = false
			} else {
				colType = jsonSchemaTypes[jsonType(currProperty.Type)].String()
			}
		}
	}
	return abstract.ColSchema{
		TableSchema:  schemaName,
		TableName:    tableName,
		Path:         "",
		ColumnName:   name,
		DataType:     colType,
		PrimaryKey:   false,
		FakeKey:      false,
		Required:     isRequired,
		Expression:   "",
		OriginalType: "",
		Properties:   nil,
	}
}

func convertTypes(in any, ytType ytschema.Type, nullable bool) (any, error) {
	if in == nil && nullable {
		return nil, nil
	}
	switch ytType {
	case ytschema.TypeBoolean:
		if out, ok := in.(bool); ok {
			return out, nil
		}
	case ytschema.TypeInt64:
		if out, ok := in.(json.Number); ok {
			return out.Int64()
		}
		if out, ok := in.(int64); ok {
			return out, nil
		}
	case ytschema.TypeFloat64:
		if out, ok := in.(json.Number); ok {
			return out, nil
		}
		if out, ok := in.(float64); ok {
			return out, nil
		}
	case ytschema.TypeString:
		if out, ok := in.(string); ok {
			return out, nil
		}
	default:
		return in, nil
	}
	return nil, xerrors.New("wrong type")
}
