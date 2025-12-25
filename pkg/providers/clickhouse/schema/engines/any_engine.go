package engines

import (
	"fmt"
	"slices"
	"strings"

	parser "github.com/transferia/transferia/pkg/providers/clickhouse/schema/ddl_parser"
)

// It can be as replicated as not-replicated
//
// actually it's anything which corresponds template `engineName()`
// where into brackets can be any amount of parameters comma-separated
// 'engineName' is anything from beginning of input string into first open bracket

type anyEngine struct {
	rawStringEnginePart string
	engineType          engineType
	params              []string
	isReplicated        bool
}

func (e *anyEngine) RawStringEnginePart() string {
	return e.rawStringEnginePart
}
func (e *anyEngine) EngineType() engineType {
	return e.engineType
}
func (e *anyEngine) Params() []string {
	return e.params
}
func (e *anyEngine) IsReplicated() bool {
	return e.isReplicated
}

func (e *anyEngine) String() string {
	return fmt.Sprintf("%v(%v)", e.engineType, strings.Join(e.params, ", "))
}

func (e *anyEngine) UpdateToNotReplicatedEngine() {
	if !e.isReplicated {
		return
	}

	if e.engineType == replicatedMergeTree {
		e.engineType = mergeTree
		e.params = nil
	} else {
		e.engineType = engineType(strings.Replace(string(e.EngineType()), "Replicated", "", 1))
		e.params = e.params[2:]
	}
	e.isReplicated = false
	e.rawStringEnginePart = ""
}

func (e *anyEngine) Clone() *anyEngine {
	return &anyEngine{
		rawStringEnginePart: e.rawStringEnginePart,
		engineType:          e.engineType,
		params:              slices.Clone(e.params),
		isReplicated:        e.isReplicated,
	}
}

func newAnyEngine(engineStrSQL string) *anyEngine {
	rawStringEnginePart, engineName, params, _ := parser.ExtractEngine(engineStrSQL)
	return &anyEngine{
		rawStringEnginePart: rawStringEnginePart,
		engineType:          engineType(engineName),
		params:              params,
		isReplicated:        isReplicatedEngineType(engineName),
	}
}
