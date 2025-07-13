package parsers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers/resources"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"golang.org/x/exp/maps"
)

type schema struct {
	Path   ypath.Path
	Fields []abstract.ColSchema `json:"fields"`
}

func adjustType(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "VT_", ""))
}

func decodeSchema(content string) ([]abstract.ColSchema, error) {
	var sch schema
	r := strings.NewReader(content)
	if err := jsonx.NewDefaultDecoder(r).Decode(&sch); err != nil {
		return nil, xerrors.Errorf("Cannot parse schema: %w", err)
	}
	if sch.Fields == nil {
		return nil, xerrors.New("No fields were found in decoded schema")
	}
	for idx, f := range sch.Fields {
		sch.Fields[idx].DataType = adjustType(f.DataType)
	}
	return sch.Fields, nil
}

func SchemaByFieldsAndResource(logger log.Logger, res resources.AbstractResources, fields []abstract.ColSchema, schemaResourceName string) ([]abstract.ColSchema, error) {
	var baseFields []abstract.ColSchema
	if len(fields) > 0 {
		baseFields = fields
	} else {
		fieldsStr, err := res.GetResource(schemaResourceName)
		if err != nil {
			return nil, xerrors.Errorf("unable to get schema from resource %s, err: %w", schemaResourceName, err)
		}
		baseFields, err = decodeSchema(fieldsStr)
		if err != nil {
			return nil, xerrors.Errorf("unable to decode schema, err: %w", err)
		}
	}
	if len(baseFields) == 0 {
		logger.Warn("No schema defined in parser config")
	}
	return baseFields, nil
}

func getFuncName(foo interface{}) string {
	fullFuncName := runtime.FuncForPC(reflect.ValueOf(foo).Pointer()).Name()
	parts := strings.Split(fullFuncName, ".")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func KnownAbstractParserConfigs() []AbstractParserConfig {
	parsers := make([]AbstractParserConfig, 0)
	for _, k := range parserConfigRegistry {
		parsers = append(parsers, k)
	}
	return parsers
}

func KnownParsersConfigs() []string {
	parsers := make([]string, 0)
	for k := range parserConfigRegistry {
		parsers = append(parsers, k)
	}
	sort.Slice(parsers, func(i int, j int) bool { return parsers[i] < parsers[j] })
	return parsers
}

func KnownParsers() []string {
	parsers := make([]string, 0)
	for k := range parsersRegistry {
		parsers = append(parsers, k)
	}
	sort.Slice(parsers, func(i int, j int) bool { return parsers[i] < parsers[j] })
	return parsers
}

func ToMap(parserName string, in interface{}) (map[string]interface{}, error) {
	resultStructured := map[string]interface{}{parserName: in}
	bytes, err := json.Marshal(resultStructured)
	if err != nil {
		return nil, xerrors.Errorf("unable to serialize")
	}
	var resultUnstructured map[string]interface{}
	err = json.Unmarshal(bytes, &resultUnstructured)
	if err != nil {
		return nil, xerrors.Errorf("unable to unmarshal")
	}
	return resultUnstructured, nil
}

func ParserConfigNameByStruct(in interface{}) string {
	result := util.ToSnakeCase(strings.TrimPrefix(strings.Split(fmt.Sprintf("%T", in), ".")[1], "ParserConfig"))
	if strings.HasSuffix(result, "_lb") {
		parserName := strings.TrimSuffix(result, "_lb")
		return parserName + ".lb"
	}
	if strings.HasSuffix(result, "_common") {
		parserName := strings.TrimSuffix(result, "_common")
		return parserName + ".common"
	}
	panic(fmt.Sprintf("%s: never should happens", result))
}

func getParserNameByStruct(in interface{}) string {
	parserConfigName := ParserConfigNameByStruct(in)
	return strings.Split(parserConfigName, ".")[0]
}

func GetParserNameByMap(parserConfig map[string]interface{}) string {
	return maps.Keys(parserConfig)[0]
}

func IsThisParserConfig(parserConfig map[string]interface{}, config interface{}) bool {
	if len(maps.Keys(parserConfig)) != 0 {
		parserName := maps.Keys(parserConfig)[0]
		return parserName == ParserConfigNameByStruct(config)
	}
	return false
}

func IsUnparsed(item abstract.ChangeItem) bool {
	return strings.HasSuffix(item.Table, "_unparsed")
}

func ExtractUnparsed(items []abstract.ChangeItem) (res []abstract.ChangeItem) {
	for _, ci := range items {
		if IsUnparsed(ci) {
			res = append(res, ci)
		}
	}

	return res
}

func ExtractErrorFromUnparsed(unparsed abstract.ChangeItem) string {
	for idx, column := range ErrParserColumns {
		if column == "_error" {
			return unparsed.ColumnValues[idx].(string)
		}
	}
	return ""
}

func VerifyUnparsed(items []abstract.ChangeItem) error {
	for _, item := range items {
		if !IsUnparsed(item) {
			continue
		}
		partID := item.TablePartID()
		cause := ExtractErrorFromUnparsed(item)
		return xerrors.Errorf("'%s' has unparsed, cause: '%s'", partID.FqtnWithPartID(), cause)
	}
	return nil
}
