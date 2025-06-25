package engines

import (
	"fmt"
	"strings"
)

type mergeTreeFamilyEngine struct {
	Type   engineType
	Params []string
}

func (e *mergeTreeFamilyEngine) IsEngine() {}

func (e *mergeTreeFamilyEngine) String() string {
	if len(e.Params) == 0 {
		return string(e.Type)
	}
	return fmt.Sprintf("%v(%v)", e.Type, strings.Join(e.Params, ", "))
}

func newMergeTreeFamilyEngine(engineStrSQL string) *mergeTreeFamilyEngine {
	result := new(mergeTreeFamilyEngine)

	paramsStart := strings.Index(engineStrSQL, "(")
	paramsEnd := strings.LastIndex(engineStrSQL, ")")
	if paramsStart > 0 && paramsEnd > 0 {
		paramsStr := strings.Trim(engineStrSQL[paramsStart+1:paramsEnd], "\r\t\n ")
		if paramsStr != "" {
			result.Params = strings.Split(paramsStr, ", ")
		}

		result.Type = engineType(engineStrSQL[:paramsStart])
	} else {
		result.Type = engineType(strings.Trim(engineStrSQL, "\n\r\t "))
	}

	return result
}
