package format

import (
	"sort"
)

const (
	int64Type   = "int64"
	int32Type   = "int32"
	int16Type   = "int16"
	int8Type    = "int8"
	uint64Type  = "uint64"
	uint32Type  = "uint32"
	uint16Type  = "uint16"
	uint8Type   = "uint8"
	float64Type = "float64"
	float32Type = "float32"
	doubleType  = "double"
	floatType   = "float"

	stringType  = "string"
	nullType    = "null"
	numberType  = "number"
	integerType = "integer"
	bytesType   = "bytes"
	boolType    = "boolean"
	arrayType   = "array"

	structType = "struct"
	objectType = "object"
)

type JSONSchemaParameters struct {
	Length                  string `json:"length,omitempty"`
	ConnectDecimalPrecision string `json:"connect.decimal.precision,omitempty"`
	Scale                   string `json:"scale,omitempty"`
	Allowed                 string `json:"allowed,omitempty"`
}

type KafkaJSONSchema struct {
	Type            string                `json:"type"`
	Fields          []KafkaJSONSchema     `json:"fields,omitempty"`
	Optional        bool                  `json:"optional"`
	Name            string                `json:"name,omitempty"`
	Version         int                   `json:"version,omitempty"`
	Doc             string                `json:"doc,omitempty"`
	Parameters      *JSONSchemaParameters `json:"parameters,omitempty"`
	Default         interface{}           `json:"default,omitempty"`
	Items           *KafkaJSONSchema      `json:"items,omitempty"`
	Field           string                `json:"field,omitempty"`
	DtOriginalTypes interface{}           `json:"__dt_original_type_info,omitempty"`
}

type ConfluentJSONSchema struct {
	ConnectIndex *int `json:"connect.index,omitempty"`

	ConnectParameters *JSONSchemaParameters `json:"connect.parameters,omitempty"`

	ConnectType          string                         `json:"connect.type,omitempty"`
	ConnectVersion       int                            `json:"connect.version,omitempty"`
	Default              interface{}                    `json:"default,omitempty"`
	Description          string                         `json:"description,omitempty"`
	Items                *ConfluentJSONSchema           `json:"items,omitempty"`
	OneOf                []ConfluentJSONSchema          `json:"oneOf,omitempty"`
	Properties           map[string]ConfluentJSONSchema `json:"properties,omitempty"`
	Title                string                         `json:"title,omitempty"`
	Type                 string                         `json:"type,omitempty"`
	DtOriginalTypes      interface{}                    `json:"__dt_original_type_info,omitempty"`
	AdditionalProperties *bool                          `json:"additionalProperties,omitempty"`
}

func confluentTypeToKafka(jsonType string, optionalType string) string {
	switch jsonType {
	case objectType:
		return structType
	case stringType:
		if optionalType == bytesType {
			return bytesType
		}
		return stringType
	case boolType:
		return boolType
	case integerType:
		return optionalType
	case numberType:
		if optionalType == float64Type {
			return doubleType
		}
		if optionalType == float32Type {
			return floatType
		}
		return bytesType
	case arrayType:
		return arrayType
	}
	return ""
}

func kafkaTypeToConfluent(fieldType string) (jsonType string, optionalType string) {
	switch fieldType {
	case int8Type, int16Type, int32Type, int64Type, uint8Type, uint16Type, uint32Type, uint64Type:
		return integerType, fieldType
	case floatType:
		return numberType, float32Type
	case doubleType:
		return numberType, float64Type
	case stringType:
		return stringType, ""
	case structType:
		return objectType, ""
	case bytesType:
		return stringType, bytesType
	case boolType:
		return boolType, ""
	case arrayType:
		return arrayType, ""
	}
	return "", ""
}

func (p ConfluentJSONSchema) ToKafkaJSONSchema() KafkaJSONSchema {
	// 'oneOf' can contain only "null" type ane one property
	for _, oneOfProperty := range p.OneOf {
		if oneOfProperty.Type == nullType {
			continue
		}
		optionalField := oneOfProperty.ToKafkaJSONSchema()
		optionalField.Optional = true
		return optionalField
	}

	type propertyWithName struct {
		name     string
		property ConfluentJSONSchema
	}
	var propertiesWithName []propertyWithName
	for name, property := range p.Properties {
		propertiesWithName = append(propertiesWithName, propertyWithName{name, property})
	}
	sort.Slice(propertiesWithName, func(i, j int) bool {
		return *propertiesWithName[i].property.ConnectIndex < *propertiesWithName[j].property.ConnectIndex
	})
	var internalFields []KafkaJSONSchema
	for _, propertyWithName := range propertiesWithName {
		field := propertyWithName.property.ToKafkaJSONSchema()
		field.Field = propertyWithName.name
		internalFields = append(internalFields, field)
	}
	var items *KafkaJSONSchema
	if p.Items != nil {
		originalItems := *p.Items
		confluentItems := originalItems.ToKafkaJSONSchema()
		items = &confluentItems
	}
	return KafkaJSONSchema{
		Type:            confluentTypeToKafka(p.Type, p.ConnectType),
		Fields:          internalFields,
		Optional:        false,
		Name:            p.Title,
		Version:         p.ConnectVersion,
		Doc:             p.Description,
		Parameters:      p.ConnectParameters,
		Default:         p.Default,
		Items:           items,
		Field:           "",
		DtOriginalTypes: p.DtOriginalTypes,
	}
}

func (p KafkaJSONSchema) ToConfluentSchema(makeClosedContentModel bool) ConfluentJSONSchema {
	return p.toConfluentSchema(0, makeClosedContentModel, false)
}

func (p KafkaJSONSchema) toConfluentSchema(depth int, makeClosedContentModel, intoAfterOrBefore bool) ConfluentJSONSchema {
	if p.Optional {
		return makeOneOfConfluentSchema(p, depth+1, makeClosedContentModel, intoAfterOrBefore)
	}

	var properties map[string]ConfluentJSONSchema
	if len(p.Fields) > 0 {
		properties = make(map[string]ConfluentJSONSchema, len(p.Fields))
		for i, field := range p.Fields {
			fieldAfterOrBefore := false
			if field.Field == "before" || field.Field == "after" {
				fieldAfterOrBefore = true
			}
			property := field.toConfluentSchema(depth+1, makeClosedContentModel, fieldAfterOrBefore)
			property.ConnectIndex = new(int)
			*property.ConnectIndex = i
			properties[field.Field] = property
		}
	}
	fieldType, connectType := kafkaTypeToConfluent(p.Type)
	var items *ConfluentJSONSchema
	if p.Items != nil {
		originalItems := *p.Items
		confluentItems := originalItems.toConfluentSchema(depth+1, makeClosedContentModel, intoAfterOrBefore)
		items = &confluentItems
	}
	var additionalProperties *bool
	if makeClosedContentModel && depth == 2 && intoAfterOrBefore {
		additionalProperties = new(bool)
		*additionalProperties = false
	}
	return ConfluentJSONSchema{
		ConnectIndex:         nil,
		ConnectParameters:    p.Parameters,
		ConnectType:          connectType,
		ConnectVersion:       p.Version,
		Default:              p.Default,
		Description:          p.Doc,
		Items:                items,
		OneOf:                nil,
		Properties:           properties,
		Title:                p.Name,
		Type:                 fieldType,
		DtOriginalTypes:      p.DtOriginalTypes,
		AdditionalProperties: additionalProperties,
	}
}

func makeOneOfConfluentSchema(p KafkaJSONSchema, depth int, makeClosedContentModel, intoAfterOrBefore bool) ConfluentJSONSchema {
	var oneOf []ConfluentJSONSchema
	oneOf = append(oneOf,
		ConfluentJSONSchema{
			ConnectIndex:         nil,
			ConnectParameters:    nil,
			ConnectType:          "",
			ConnectVersion:       0,
			Default:              nil,
			Description:          "",
			Items:                nil,
			OneOf:                nil,
			Properties:           nil,
			Title:                "",
			Type:                 nullType,
			DtOriginalTypes:      nil,
			AdditionalProperties: nil,
		},
	)
	p.Optional = false
	oneOf = append(oneOf, p.toConfluentSchema(depth, makeClosedContentModel, intoAfterOrBefore))
	return ConfluentJSONSchema{
		ConnectIndex:         nil,
		ConnectParameters:    nil,
		ConnectType:          "",
		ConnectVersion:       0,
		Default:              nil,
		Description:          "",
		Items:                nil,
		OneOf:                oneOf,
		Properties:           nil,
		Title:                "",
		Type:                 "",
		DtOriginalTypes:      nil,
		AdditionalProperties: nil,
	}
}
