package generic

import (
	"maps"
	"strings"

	"github.com/goccy/go-json"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// makeRest returns unmapped fields without modifying its inputs.
func makeRest(
	item map[string]interface{},
	known map[string]bool,
	fields []abstract.ColSchema,
	ignoreColumnPaths bool,
) map[string]interface{} {
	rest := make(map[string]interface{})
	for key, value := range item {
		if !known[key] {
			rest[key] = value
		}
	}
	if ignoreColumnPaths {
		return rest
	}
	for _, col := range fields {
		if col.IsNestedKey() {
			removeRestPath(rest, splitColumnPath(col.Path))
		}
	}
	return rest
}

// removeRestPath prunes only ancestors emptied by removing an existing field.
// Nested maps may also be used by output columns, so copy them before editing.
func removeRestPath(rest map[string]interface{}, path []string) bool {
	key := path[0]
	value, exists := rest[key]
	if !exists {
		return false
	}
	if len(path) == 1 {
		delete(rest, key)
		return true
	}

	var nested map[string]interface{}
	switch value := value.(type) {
	case map[string]interface{}:
		nested = maps.Clone(value)
	case string:
		var err error
		nested, err = parseRestJSON(value)
		if err != nil {
			return false
		}
	default:
		return false
	}
	if !removeRestPath(nested, path[1:]) {
		return false
	}
	if len(nested) == 0 {
		delete(rest, key)
		return true
	}
	if _, encoded := value.(string); encoded {
		data, err := json.Marshal(nested)
		if err != nil {
			return false
		}
		rest[key] = string(data)
		return true
	}
	rest[key] = nested
	return true
}

func parseRestJSON(value string) (map[string]interface{}, error) {
	if !json.Valid([]byte(value)) {
		// Match lookupComplex's handling of legacy escaped JSON strings.
		return parseJSON(value)
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	// Re-encoding an unmarked number must not round it through float64.
	decoder.UseNumber()
	var result map[string]interface{}
	if err := decoder.Decode(&result); err != nil {
		return nil, xerrors.Errorf("unable to parse rest JSON: %w", err)
	}
	return result, nil
}
