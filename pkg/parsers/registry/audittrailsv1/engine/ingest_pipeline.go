package engine

import (
	_ "embed"
	"encoding/json"
	"net"
	"net/url"
	"regexp"
	"strings"

	"github.com/ohler55/ojg/jp"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/stringutil"
	"golang.org/x/exp/maps"
)

type ingestPipelineCommandArgs struct {
	Field       string `json:"field"`
	TargetField string `json:"target_field"`
	Value       string `json:"value"`
	If          string `json:"if"`
}

type ingestPipelineProgram struct {
	Description string
	Processors  []map[string]ingestPipelineCommandArgs
}

var program ingestPipelineProgram

//go:embed ingest_pipeline.json
var ingestPipeline []byte

func init() {
	_ = json.Unmarshal(ingestPipeline, &program)
}

func makeEmptyChain(keySteps []string, val interface{}) interface{} {
	currMap := make(map[string]interface{})
	currMap[keySteps[len(keySteps)-1]] = val
	for i := 0; i < len(keySteps)-1; i++ {
		prefixLength := len(keySteps) - i - 2
		currStep := keySteps[prefixLength]
		newMap := make(map[string]interface{})
		newMap[currStep] = currMap
		currMap = newMap
	}
	return currMap
}

var reArr = regexp.MustCompile(`\.([0-9]+)\.`)

func jsonpathGet(key string, in map[string]interface{}) (interface{}, error) {
	jsonPathKey := reArr.ReplaceAllString(key, "[$1].")
	expr := jp.MustParseString(jsonPathKey)
	result := expr.Get(in)
	if len(result) == 0 {
		return nil, nil
	} else if len(result) == 1 {
		return result[0], nil
	} else {
		arr, _ := json.Marshal(in)
		return nil, xerrors.Errorf("impossible case: len(result)>1, jsonPath:%s, json:%s", jsonPathKey, arr)
	}
}

func jsonPathSet(in map[string]interface{}, key string, val interface{}) {
	keySteps := strings.Split(key, ".")
	currPtr := in
	for i := 0; i < len(keySteps); i++ {
		currStep := keySteps[i]
		currVal, ok := currPtr[currStep]

		var isCurrStepMap bool
		switch currVal.(type) {
		case map[string]interface{}:
			isCurrStepMap = true
		default:
			isCurrStepMap = false
		}

		if (!ok) || (!isCurrStepMap) {
			if i == len(keySteps)-1 {
				currPtr[currStep] = val
			} else {
				currPtr[currStep] = makeEmptyChain(keySteps[i+1:], val)
			}
			break
		} else {
			currPtr = currVal.(map[string]interface{})
		}
	}
}

func jsonPathDel(in map[string]interface{}, key string) {
	keySteps := strings.Split(key, ".")
	currPtr := in
	for i := 0; i < len(keySteps); i++ {
		currStep := keySteps[i]
		currVal, ok := in[currStep]
		if i == len(keySteps)-1 {
			delete(currPtr, currStep)
			break
		}
		if !ok {
			break
		} else {
			currPtr = currVal.(map[string]interface{})
		}
	}
}

func isMustacheValue(field string) bool {
	return strings.HasPrefix(field, "{{{") && strings.HasSuffix(field, "}}}")
}

func extractMustacheExpr(val string) string {
	return strings.TrimSuffix(strings.TrimPrefix(val, "{{{"), "}}}")
}

func isIPAddressValid(ip string) bool {
	if net.ParseIP(ip) == nil {
		return false
	} else {
		return true
	}
}

func conditionIsTrue(in ingestPipelineCommandArgs, myMap map[string]interface{}) (bool, error) {
	switch in.If {
	case `ctx.event.status == 'DONE'`:
		v, _ := jsonpathGet("event.status", myMap) // 'ctx' - it's like elasticsearch 'keyword' to access root element of json document - so, we emit it into jsonpath
		switch unwrapVal := v.(type) {
		case string:
			return unwrapVal == "DONE", nil
		default:
			return false, nil
		}
	case `ctx.request_metadata.remote_address != 'cloud.yandex'`:
		v, _ := jsonpathGet("request_metadata.remote_address", myMap) // 'ctx' - it's like elasticsearch 'keyword' to access root element of json document - so, we emit it into jsonpath
		switch unwrapVal := v.(type) {
		case string:
			return isIPAddressValid(unwrapVal), nil
		case nil:
			return true, nil
		default:
			return false, nil
		}
	default:
		return false, xerrors.Errorf("unknown condition: %s", in.If)
	}
}

func exec(in string, inProgram *ingestPipelineProgram) (string, error) {
	var myMap map[string]interface{}
	err := json.Unmarshal([]byte(in), &myMap)
	if err != nil {
		return "", xerrors.Errorf("unable to unmarshal json with input data. input data: %s", stringutil.TruncateUTF8(in, 1024))
	}
	for _, currMap := range inProgram.Processors {
		processor := maps.Keys(currMap)[0]
		args := maps.Values(currMap)[0]
		switch processor {
		case "rename":
			// doc: If the field doesn’t exist or the new name is already used, an exception will be thrown.
			v, _ := jsonpathGet(args.TargetField, myMap)
			if v != nil {
				return "", xerrors.Errorf("rename failed, target field already exists: %s", args.TargetField)
			}
			val, err := jsonpathGet(args.Field, myMap)
			if err != nil {
				return "", xerrors.Errorf("unable to extract json value by path in processor 'rename', err: %w", err)
			}
			if val == nil {
				continue
			}
			jsonPathSet(myMap, args.TargetField, val)
			jsonPathDel(myMap, args.Field)
		case "urldecode":
			val, err := jsonpathGet(args.Field, myMap)
			if err != nil {
				return "", xerrors.Errorf("unable to extract json value by path in processor 'urldecode', err: %w", err)
			}
			if val == nil {
				continue
			}
			decodedValue, err := url.QueryUnescape(val.(string))
			if err != nil {
				return "", xerrors.Errorf("unable to QueryUnescape in processor 'urldecode', err: %w", err)
			}
			jsonPathSet(myMap, args.Field, decodedValue)
		case "set":
			if args.If != "" {
				cond, err := conditionIsTrue(args, myMap)
				if err != nil {
					return "", xerrors.Errorf("unable to check condition, err: %w", err)
				}
				if !cond {
					continue
				}
			}
			if isMustacheValue(args.Value) {
				expr := extractMustacheExpr(args.Value)
				val, err := jsonpathGet(expr, myMap)
				if err != nil {
					return "", xerrors.Errorf("unable to extract json value by path in processor 'set', err: %w", err)
				}
				if val == nil {
					continue
				}
				jsonPathSet(myMap, args.Field, val)
			} else {
				jsonPathSet(myMap, args.Field, args.Value)
			}
		case "geoip":
			// doc: The geoip processor adds information about the geographical location of an IPv4 or IPv6 address.
			// by default, it stores geolocation into field 'geoip'
			continue
		default:
			return "", xerrors.Errorf("unknown processor: %s", processor)
		}
	}
	result, err := json.Marshal(myMap)
	if err != nil {
		return "", xerrors.Errorf("unable to marshal json, err: %w", err)
	}
	return string(result), nil
}

func ExecProgram(in string) (string, error) {
	return exec(in, &program)
}
