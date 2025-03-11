package engine

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/schemaregistry/confluent"
	"github.com/transferria/transferria/pkg/util"
	"github.com/transferria/transferria/pkg/util/jsonx"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type JSONProperties struct {
	Type       string                     `json:"type"`
	OneOf      []*JSONProperties          `json:"oneOf"`
	Properties map[string]*JSONProperties `json:"properties"`
	Title      string                     `json:"title"`
}

func makeChangeItemsFromMessageWithJSON(schema *confluent.Schema, buf []byte, offset uint64, writeTime time.Time) ([]abstract.ChangeItem, int, error) {
	var jsonProperties JSONProperties
	err := json.Unmarshal([]byte(schema.Schema), &jsonProperties)
	if err != nil {
		return nil, 0, xerrors.Errorf("Can't unmarshal JSON schema %q: %w", schema.Schema, err)
	}

	separatedTitle := strings.SplitN(jsonProperties.Title, ".", 2)
	if len(separatedTitle) != 2 {
		return nil, 0, xerrors.Errorf("Can't split title %q from json into schema and table names", jsonProperties.Title)
	}
	schemaName := separatedTitle[0]
	tableName := separatedTitle[1]

	msgLen := len(buf)
	zeroIndex := bytes.Index(buf, []byte{0})
	if zeroIndex != -1 {
		msgLen = zeroIndex
	}

	tableColumns, names, values, err := processPayload(schemaName, tableName, &jsonProperties, buf[0:msgLen])
	if err != nil {
		return nil, 0, xerrors.Errorf("Can't process payload:%w", err)
	}
	changeItem := abstract.ChangeItem{
		ID:           0,
		LSN:          offset,
		CommitTime:   uint64(writeTime.UnixNano()),
		Counter:      0,
		Kind:         abstract.UpdateKind,
		Schema:       schemaName,
		Table:        tableName,
		PartID:       "",
		ColumnNames:  names,
		ColumnValues: values,
		TableSchema:  tableColumns,
		OldKeys:      abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		TxID:         "",
		Query:        "",
		Size:         abstract.RawEventSize(uint64(len(buf))),
	}
	return []abstract.ChangeItem{changeItem}, msgLen, nil
}

func processPayload(schemaName, tableName string, jsonSchema *JSONProperties, payload []byte) (*abstract.TableSchema, []string, []interface{}, error) {
	var rows []abstract.ColSchema
	var names []string
	var values []interface{}

	if jsonSchema == nil {
		return nil, nil, nil, xerrors.Errorf("json schema can't be empty")
	}
	if jsonSchema.Type != "object" {
		return nil, nil, nil, xerrors.Errorf("json schema type must be 'object'")
	}

	schemaRowNames := util.MapKeysInOrder(jsonSchema.Properties)
	for _, name := range schemaRowNames {
		rows = append(rows, jsonPropertyToJSONSchemaRow(schemaName, tableName, name, jsonSchema.Properties[name]))
	}
	var dataChanges map[string]interface{}
	if err := jsonx.NewDefaultDecoder(bytes.NewReader(payload)).Decode(&dataChanges); err != nil {
		return nil, nil, nil, xerrors.Errorf("Can't unmarshal data changes from message: %w", err)
	}
	for _, row := range rows {
		if value, ok := dataChanges[row.ColumnName]; ok {
			names = append(names, row.ColumnName)
			converted, err := convertTypes(value, ytschema.Type(row.DataType), !row.Required)
			if err != nil {
				return nil, nil, nil, xerrors.Errorf("Can't convert %q value %v (type %T) to type %q: %w", row.ColumnName, value, value, ytschema.Type(row.DataType), err)
			}
			values = append(values, converted)
		} else if row.Required {
			return nil, nil, nil, xerrors.Errorf("Field %q is required, but not found in payload %q", row.ColumnName, string(payload))
		}
	}

	return abstract.NewTableSchema(rows), names, values, nil
}

func jsonPropertyToJSONSchemaRow(schemaName, tableName, name string, property *JSONProperties) abstract.ColSchema {
	colType := jsonSchemaTypes[jsonType(property.Type)].String()
	isRequired := true
	if property.OneOf != nil {
		for _, property := range property.OneOf {
			if property.Type == JSONTypeNull.String() {
				isRequired = false
			} else {
				colType = jsonSchemaTypes[jsonType(property.Type)].String()
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
