package parsers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/stats"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

//--- ParserConfig parserConfigRegistry

type parserFactory func(interface{}, bool, log.Logger, *stats.SourceStats) (Parser, error)

var parsersRegistry = make(map[string]parserFactory)

var parserConfigRegistry = make(map[string]AbstractParserConfig)

var UnknownParserErr = xerrors.NewSentinel("unregistered parser")

func Register(foo parserFactory, configs []AbstractParserConfig) {
	parserName := util.ToSnakeCase(strings.TrimPrefix(getFuncName(foo), "NewParser"))
	parsersRegistry[parserName] = foo

	for _, config := range configs {
		parserConfigName := ParserConfigNameByStruct(config)
		parserConfigRegistry[parserConfigName] = config

		parserNameCheck := getParserNameByStruct(config)
		if parserName != parserNameCheck {
			panic(fmt.Sprintf("inconsistent Register input: %s!=%s", parserName, parserNameCheck))
		}
	}
}

func NewParserFromParserConfig(parserConfig AbstractParserConfig, sniff bool, logger log.Logger, stats *stats.SourceStats) (Parser, error) {
	parserName := getParserNameByStruct(parserConfig)
	if parserFactory, ok := parsersRegistry[parserName]; !ok {
		return nil, xerrors.Errorf("unable to find parser %s", parserName)
	} else {
		return parserFactory(parserConfig, sniff, logger, stats)
	}
}

func NewParserFromMap(in map[string]interface{}, sniff bool, logger log.Logger, stats *stats.SourceStats) (Parser, error) {
	parserConfig, err := ParserConfigMapToStruct(in)
	if err != nil {
		return nil, xerrors.Errorf("unable create parserConfig, err: %w", err)
	}
	return NewParserFromParserConfig(parserConfig, sniff, logger, stats)
}

//--- MAIN INTERFACE FUNCTIONS

func ParserConfigStructToMap(in AbstractParserConfig) (map[string]interface{}, error) {
	if fmt.Sprintf("%T", in)[0] == '*' {
		result, err := ToMap(ParserConfigNameByStruct(in), in)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert struct to map, err: %w", err)
		}
		return result, nil
	}
	result, err := ToMap(ParserConfigNameByStruct(in), in)
	if err != nil {
		return nil, xerrors.Errorf("unable to convert struct to map, err: %w", err)
	}
	return result, nil
}

func ParserConfigMapToStruct(in map[string]interface{}) (AbstractParserConfig, error) {
	if in == nil {
		return nil, xerrors.Errorf("unable to convert parser_config map to struct, when map is nil")
	}
	if len(in) != 1 {
		return nil, xerrors.Errorf("ParserConfigMapToStruct: len(in) != 1")
	}
	parserConfigName := ""
	var parserParams interface{} = nil
	for k, v := range in {
		parserConfigName = k
		parserParams = v
	}

	registeredStruct, ok := parserConfigRegistry[parserConfigName]
	if !ok {
		return nil, xerrors.Errorf("parser_config name: %s, known_parsers: %v: %w", parserConfigName, KnownParsers(), UnknownParserErr)
	}
	pointerToRegisteredStructCopy := reflect.New(reflect.ValueOf(registeredStruct).Type().Elem()).Interface()

	bytes, err := json.Marshal(parserParams)
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal json, err: %w", err)
	}
	err = json.Unmarshal(bytes, pointerToRegisteredStructCopy)
	if err != nil {
		return nil, xerrors.Errorf("unable to unmarshal json, err: %w", err)
	}
	return pointerToRegisteredStructCopy.(AbstractParserConfig), nil
}
