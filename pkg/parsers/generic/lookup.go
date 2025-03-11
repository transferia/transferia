package generic

import (
	"strings"

	"github.com/goccy/go-json"
	"github.com/transferria/transferria/library/go/core/xerrors"
)

func lookupComplex(obj interface{}, path string) (interface{}, error) {
	fieldNames := strings.Split(path, ".")
	if len(fieldNames) == 1 {
		fieldNames = strings.Split(path, "/")
	}

	for _, fieldName := range fieldNames {
		var m map[string]interface{}
		switch value := obj.(type) {
		case map[string]interface{}:
			m = value
		case string:
			var err error
			m, err = parseJSON(value)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse json: %w", err)
			}
		default:
			return nil, xerrors.Errorf("unexpected value type: %T", value)
		}

		var ok bool
		obj, ok = m[fieldName]
		if !ok {
			return nil, xerrors.Errorf("unable to get field: %s", fieldName)
		}
	}

	return obj, nil
}

func parseJSON(s string) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(s), &result); err == nil {
		return result, nil
	}

	// TODO(@tserakhau): possible double escape need to fix it
	s = strings.ReplaceAll(s, "\\\\\"", "\\\"")
	if err := json.Unmarshal([]byte(s), &result); err == nil {
		return result, nil
	}

	s = strings.ReplaceAll(s, "\\", "")
	if err := json.Unmarshal([]byte(s), &result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}
