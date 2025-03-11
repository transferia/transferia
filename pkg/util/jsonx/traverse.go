package jsonx

import (
	"encoding/json"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

type KVReplacer = func(path, k string, v any) (string, any, bool)

func RecursiveTraverseUnmarshalledJSON(path string, in any, currKVReplacer KVReplacer) (any, error) {
	buildPath := func(path, k string) string {
		if path == "" {
			return k
		}
		return path + "." + k
	}

	switch inVal := in.(type) {
	case map[string]any:
		result := make(map[string]any)
		for k, v := range inVal {
			currPath := buildPath(path, k)
			switch vv := v.(type) {
			case map[string]any:
				newV, err := RecursiveTraverseUnmarshalledJSON(currPath, vv, currKVReplacer)
				if err != nil {
					return nil, xerrors.Errorf("unable to recursive traverse unmarshalled json, err: %w", err)
				}
				newK, newVV, isSave := currKVReplacer(currPath, k, newV)
				if isSave {
					result[newK] = newVV
				}
			case []any:
				newV, err := RecursiveTraverseUnmarshalledJSON(currPath, vv, currKVReplacer)
				if err != nil {
					return nil, xerrors.Errorf("unable to recursive traverse unmarshalled json, err: %w", err)
				}
				newK, newVV, isSave := currKVReplacer(currPath, k, newV)
				if isSave {
					result[newK] = newVV
				}
			default:
				newK, newV, isSave := currKVReplacer(currPath, k, v)
				if isSave {
					result[newK] = newV
				}
			}
		}
		return result, nil
	case []any:
		result := make([]any, 0)
		for _, v := range inVal {
			el, err := RecursiveTraverseUnmarshalledJSON(path+"[]", v, currKVReplacer)
			if err != nil {
				return nil, xerrors.Errorf("unable to recursive traverse unmarshalled json, err: %w", err)
			}
			result = append(result, el)
		}
		return result, nil
	case int, int8, int16, int32, int64, uint, uint8, uint32, uint64, float32, float64, json.Number:
		return in, nil
	case string:
		return in, nil
	case bool:
		return in, nil
	default:
		return nil, xerrors.Errorf("unknown type in traversing unmarshalled json, type:%T", in)
	}
}
