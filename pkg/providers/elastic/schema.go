//go:build !disable_elastic_provider

package elastic

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/util/jsonx"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	idColumn             = "_id"
	fieldFormatSchemaKey = abstract.PropertyKey("elasticsearch:fieldFormatSchemaKey")
)

type SchemaDescription struct {
	Columns      []abstract.ColSchema
	ColumnsNames []string
}

func (s *Storage) getSchemaFromElasticMapping(mappings mappingProperties, isHomo bool) (*SchemaDescription, error) {
	aliasType := "alias"
	objectType := "object"
	columnNames := []string{idColumn}

	// add the id as first column
	cols := []abstract.ColSchema{{
		ColumnName:   idColumn,
		DataType:     ytschema.TypeString.String(),
		PrimaryKey:   true,
		OriginalType: fmt.Sprintf("%s:%s", ProviderType, "text"),
	}}

	rules := typesystem.RuleFor(ProviderType).Source

	var schemaDescription SchemaDescription

	keys := sortedMappingKeys(mappings.Properties)

	for _, key := range keys {
		fieldName := key
		field := mappings.Properties[key]

		var schemaType ytschema.Type
		var originalType string

		if field.Type == "" && field.Properties != nil {
			ok := false
			// object type
			schemaType, ok = rules[objectType]
			if !ok {
				return nil, xerrors.Errorf("failed to find type mapping for provider type %s", objectType)
			}
			originalType = objectType
		}
		if field.Type != "" {
			ok := false
			if field.Type == aliasType && field.Path != "" {
				actualType, err := getOriginalTypeFromAliasField(mappings, field.Path)
				if err != nil {
					return nil, xerrors.Errorf("failed to find actual type for alias with path %s", field.Path)
				}

				schemaType, ok = rules[actualType]
				if !ok {
					return nil, xerrors.Errorf("failed to find type mapping for provider type %s", actualType)
				}
				originalType = aliasType
			} else {
				schemaType, ok = rules[field.Type]
				if !ok {
					return nil, xerrors.Errorf("failed to find type mapping for provider type %s", field.Type)
				}
				originalType = field.Type
			}
		}

		colSchema := new(abstract.ColSchema)
		colSchema.ColumnName = fieldName
		if isHomo {
			colSchema.DataType = ytschema.TypeAny.String()
		} else {
			colSchema.DataType = string(schemaType)
		}
		colSchema.OriginalType = fmt.Sprintf("%s:%s", ProviderType, originalType)
		if field.Format != "" {
			colSchema.AddProperty(fieldFormatSchemaKey, strings.Split(field.Format, "||"))
		}
		columnNames = append(columnNames, fieldName)
		cols = append(cols, *colSchema)
	}

	schemaDescription.ColumnsNames = columnNames
	schemaDescription.Columns = cols

	return &schemaDescription, nil
}

func getOriginalTypeFromAliasField(mappings mappingProperties, pathToOriginal string) (string, error) {
	// example path could be nested objet user.name and original type is stored in name
	subPaths := strings.Split(pathToOriginal, ".")

	var currentMapping mappingType
	var rawMapping map[string]json.RawMessage

	for index, path := range subPaths {
		if index == 0 {
			mapping, ok := mappings.Properties[path]
			if !ok {
				return "", xerrors.Errorf("missing original type mapping for alias")
			}

			currentMapping = mapping
		} else {
			mapping, ok := rawMapping[path]
			if !ok {
				return "", xerrors.Errorf("missing original type mapping for alias")
			}

			if err := jsonx.Unmarshal(mapping, &currentMapping); err != nil {
				return "", xerrors.Errorf("failed to unmarshal currentMapping :%w", err)
			}
		}

		if index == (len(subPaths) - 1) {
			if currentMapping.Type != "" {
				return currentMapping.Type, nil
			}
			return "", xerrors.Errorf("missing original type mapping for alias")
		}

		if currentMapping.Properties != nil {
			mapping, ok := currentMapping.Properties[path]
			if !ok {
				return "", xerrors.Errorf("missing original type mapping for alias")
			}

			if err := jsonx.Unmarshal(mapping, &rawMapping); err != nil {
				return "", xerrors.Errorf("failed to unmarshal rawMapping :%w", err)
			}
		}
	}

	return "", xerrors.Errorf("missing original type mapping for alias")
}

func (s *Storage) fixDataTypesWithSampleData(index string, schemaDescription *SchemaDescription) error {
	body, err := getResponseBody(s.Client.Search(
		s.Client.Search.WithSize(1),
		s.Client.Search.WithBody(strings.NewReader(`{
			"sort": [{"_id": "asc"}]
		}`)),
		s.Client.Search.WithIndex(index)))
	if err != nil {
		return xerrors.Errorf("unable to fetch sample document, index: %s, err: %w", index, err)
	}

	var result searchResponse
	if err := jsonx.Unmarshal(body, &result); err != nil {
		return xerrors.Errorf("failed to unmarshal sample document, index: %s, err: %w", index, err)
	}

	if len(result.Hits.Hits) != 0 {
		var doc map[string]interface{}

		if err := jsonx.Unmarshal(result.Hits.Hits[0].Source, &doc); err != nil {
			return err
		}
		var amendedColumns []abstract.ColSchema
		for _, column := range schemaDescription.Columns {
			amended := false
			for fieldName, value := range doc {
				if value == nil {
					continue
				}
				// check for possible array
				if (reflect.TypeOf(value).Kind() == reflect.Slice) || (reflect.TypeOf(value).Kind() == reflect.Array) {
					// field is actually an array, check if field is not type any and amend
					if column.ColumnName == fieldName && column.DataType != ytschema.TypeAny.String() {

						col := new(abstract.ColSchema)
						col.ColumnName = column.ColumnName
						col.DataType = ytschema.TypeAny.String()
						col.OriginalType = column.OriginalType

						amendedColumns = append(amendedColumns, *col)
						amended = true
					}
				}
			}

			if !amended {
				amendedColumns = append(amendedColumns, column)
			}
		}
		schemaDescription.Columns = amendedColumns
	}

	return nil
}

func sortedMappingKeys(mappings map[string]mappingType) []string {
	keys := make([]string, 0, len(mappings))
	for k := range mappings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
