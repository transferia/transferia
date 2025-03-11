package engine

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/parsers/generic"
	jsonparser "github.com/transferria/transferria/pkg/parsers/registry/json"
	"github.com/transferria/transferria/pkg/stats"
	"github.com/transferria/transferria/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
)

type AuditTrailsV1ParserImpl struct {
	useElasticSchema bool
	parser           parsers.Parser
}

func (p *AuditTrailsV1ParserImpl) parseLine(line string) (map[string]any, error) {
	finalLine, err := ExecProgram(line)
	if err != nil {
		return nil, xerrors.Errorf("unable to exec pipeline program: %w", err)
	}
	result, err := makeHungarianNotation(finalLine)
	if err != nil {
		return nil, xerrors.Errorf("unable to make hungarian notation, err: %w", err)
	}
	return result, nil
}

var knownProblemPaths = map[string]bool{
	"details.backup": true,
	"details.cluster.config.disk_size_autoscaling.disk_size_limit":           true,
	"details.cluster.config.disk_size_autoscaling.emergency_usage_threshold": true,
	"details.cluster.config.disk_size_autoscaling.planned_usage_threshold":   true,
	"details.database":           true,
	"details.domain_id":          true,
	"details.enabled":            true,
	"details.endpoint":           true,
	"details.hosts.priority":     true,
	"details.id":                 true,
	"details.interface_id":       true,
	"details.maintenance_policy": true,
	"details.options":            true,
	"details.origin":             true,
	"details.origin_group_id":    true,
	"details.tags":               true,
	"details.verdict":            true,
	"details.version":            true,
	"request_parameters.backup":  true,
	"request_parameters.config_spec.disk_size_autoscaling.disk_size_limit":           true,
	"request_parameters.config_spec.disk_size_autoscaling.emergency_usage_threshold": true,
	"request_parameters.config_spec.disk_size_autoscaling.planned_usage_threshold":   true,
	"request_parameters.deletion_protection":                                         true,
	"request_parameters.enabled":                                                     true,
	"request_parameters.group_name":                                                  true,
	"request_parameters.host_specs.priority":                                         true,
	"request_parameters.host_specs.priority.value":                                   true,
	"request_parameters.id":                                                          true,
	"request_parameters.login":                                                       true,
	"request_parameters.maintenance_policy":                                          true,
	"request_parameters.master_spec.version":                                         true,
	"request_parameters.metadata":                                                    true,
	"request_parameters.origin_group_id":                                             true,
	"request_parameters.pinned":                                                      true,
	"request_parameters.position":                                                    true,
	"request_parameters.runtime":                                                     true,
	"request_parameters.tags":                                                        true,
	"request_parameters.target":                                                      true,
	"request_parameters.update_host_specs.assign_public_ip":                          true,
	"request_parameters.update_host_specs.priority":                                  true,
	"request_parameters.update_host_specs.priority.value":                            true,
	"request_parameters.version":                                                     true,
	"response.id":                                                                    true,
}

func determineSuffix(in any) string {
	valueOf := reflect.ValueOf(in)
	valType := valueOf.Kind()
	if valType == reflect.Map {
		return "obj"
	}

	if valType == reflect.Array || valType == reflect.Slice {
		if valueOf.Len() == 0 {
			return ""
		}
		return "arr__" + determineSuffix(valueOf.Index(0).Interface())
	}

	switch in.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint32, uint64, float32, float64, json.Number:
		return "num"
	case string:
		return "str"
	case bool:
		return "bool"
	}
	return ""
}

func makeHungarianNotation(in string) (map[string]any, error) {
	var myMap map[string]any
	err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&myMap)
	if err != nil {
		return nil, xerrors.Errorf("unable to decode json, err:%w, json:%s", err, in)
	}
	resultDict, err := jsonx.RecursiveTraverseUnmarshalledJSON("", myMap, func(path, k string, v any) (string, any, bool) {
		handleKnownProblemPaths := func(path, k string, v any) (string, any) {
			if _, ok := knownProblemPaths[path]; !ok {
				return k, v
			}
			suffix := determineSuffix(v)
			return k + "__" + suffix, v
		}
		newK, newV := handleKnownProblemPaths(path, k, v)
		newK = strings.ReplaceAll(newK, ".", "_")
		if newVStr, ok := newV.(string); ok && newVStr == "*** hidden ***" {
			return "", nil, false
		}
		return newK, newV, true
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to traverse recursive unmarshalled json, err:%w, json:%s", err, in)
	}
	if result, ok := resultDict.(map[string]any); ok {
		return result, nil
	}
	return nil, xerrors.Errorf("unknown type of resultDict: %T", resultDict)
}

func (p *AuditTrailsV1ParserImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	if !p.useElasticSchema {
		return p.parser.Do(msg, partition)
	}

	lines := strings.Split(string(msg.Value), "\n")
	result := make([]abstract.ChangeItem, 0, len(lines))

	for i, line := range lines {
		dict, err := p.parseLine(line)
		if err != nil {
			result = append(result, generic.NewUnparsed(
				partition, partition.Topic, line, err.Error(), i, msg.Offset, msg.WriteTime,
			))
			continue
		}

		changeItem := abstract.ChangeItem{
			ID:           0,
			LSN:          msg.Offset,
			CommitTime:   uint64(msg.WriteTime.UnixNano()),
			Counter:      0,
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        strings.Replace(partition.Topic, "/", "_", -1),
			PartID:       "",
			ColumnNames:  make([]string, 0, len(dict)),
			ColumnValues: make([]any, 0, len(dict)),
			TableSchema:  getElasticFields(),
			OldKeys:      abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(uint64(len(line))),
		}

		for k, v := range dict {
			changeItem.ColumnNames = append(changeItem.ColumnNames, k)
			changeItem.ColumnValues = append(changeItem.ColumnValues, v)
		}
		result = append(result, changeItem)
	}
	return result
}

func (p *AuditTrailsV1ParserImpl) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0, 1000)
	for _, msg := range batch.Messages {
		result = append(result, p.Do(msg, abstract.Partition{Cluster: "", Partition: batch.Partition, Topic: batch.Topic})...)
	}
	return result
}

func makeColSchemaWithoutPath(name, type_ string) abstract.ColSchema {
	return makeColSchema(name, "", type_)
}

func makeColSchemaImpl(name, path, type_ string, pkey bool) abstract.ColSchema {
	return abstract.ColSchema{
		TableSchema:  "",
		TableName:    "",
		Path:         path,
		ColumnName:   name,
		DataType:     type_,
		PrimaryKey:   pkey,
		FakeKey:      false,
		Required:     false,
		Expression:   "",
		OriginalType: "",
		Properties:   nil,
	}
}

func makeColSchema(name, path, type_ string) abstract.ColSchema {
	return makeColSchemaImpl(name, path, type_, false)
}

func makeColSchemaPK(name, path, type_ string) abstract.ColSchema {
	return makeColSchemaImpl(name, path, type_, true)
}

func getNotElasticFields() []abstract.ColSchema {
	return []abstract.ColSchema{
		makeColSchemaPK("event_id", "event_id", "string"),
		makeColSchema("event_source", "event_source", "string"),
		makeColSchema("event_type", "event_type", "string"),
		makeColSchema("event_time", "event_time", "datetime"),
		makeColSchema("authenticated", "authentication.authenticated", "any"),
		makeColSchema("subject_type", "authentication.subject_type", "string"),
		makeColSchema("subject_id", "authentication.subject_id", "string"),
		makeColSchema("subject_name", "authentication.subject_name", "string"),
		makeColSchema("authorized", "authorization.authorized", "any"),
		makeColSchema("remote_address", "request_metadata.remote_address", "string"),
		makeColSchema("user_agent", "request_metadata.user_agent", "string"),
		makeColSchema("request_id", "request_metadata.request_id", "string"),
		makeColSchema("event_status", "event_status", "string"),
		makeColSchema("details", "details", "any"),
		makeColSchema("org_id", "resource_metadata..path[0].resource_id", "string"),
		makeColSchema("org_name", "resource_metadata..path[0].resource_name", "string"),
		makeColSchema("cloud_id", "resource_metadata..path[1].resource_id", "string"),
		makeColSchema("cloud_name", "resource_metadata..path[1].resource_name", "string"),
		makeColSchema("folder_id", "resource_metadata..path[2].resource_id", "string"),
		makeColSchema("folder_name", "resource_metadata..path[2].resource_name", "string"),
		makeColSchema("request_parameters", "request_parameters", "any"),
		makeColSchema("operation_id", "response.operation_id", "string"),
	}
}

// there are a lot of unused columns, which appear after renaming in `ingest_pipeline.json`
func getElasticFields() *abstract.TableSchema {
	return abstract.NewTableSchema([]abstract.ColSchema{
		makeColSchemaPK("event_id", "", "string"),
		makeColSchemaWithoutPath("@timestamp", "datetime"),
		makeColSchemaWithoutPath("authentication", "any"),
		makeColSchemaWithoutPath("authorization", "any"),
		makeColSchemaWithoutPath("cloud", "any"),
		makeColSchemaWithoutPath("details", "any"),
		makeColSchemaWithoutPath("event", "any"),
		makeColSchemaWithoutPath("event_source", "string"),
		makeColSchemaWithoutPath("event_status", "string"),
		makeColSchemaWithoutPath("event_time", "datetime"),
		makeColSchemaWithoutPath("event_type", "string"),
		makeColSchemaWithoutPath("object_storage", "any"),
		makeColSchemaWithoutPath("request_metadata", "any"),
		makeColSchemaWithoutPath("resource_metadata", "any"),
		makeColSchemaWithoutPath("security_group", "any"),
		makeColSchemaWithoutPath("source", "any"),
		makeColSchemaWithoutPath("user", "any"),
		makeColSchemaWithoutPath("user_agent", "any"),
		makeColSchemaWithoutPath("request_parameters", "any"),
		makeColSchemaWithoutPath("operation_id", "string"),
	})
}

func NewAuditTrailsV1ParserImpl(
	useElasticSchema, sniff bool, logger log.Logger, registry *stats.SourceStats,
) (*AuditTrailsV1ParserImpl, error) {

	res := &AuditTrailsV1ParserImpl{
		useElasticSchema: useElasticSchema,
		parser:           nil,
	}

	if !useElasticSchema {
		config := &jsonparser.ParserConfigJSONCommon{
			Fields:               getNotElasticFields(),
			SchemaResourceName:   "",
			NullKeysAllowed:      false,
			AddRest:              false,
			AddDedupeKeys:        false,
			UseNumbersInAny:      false,
			UnescapeStringValues: false,
			UnpackBytesBase64:    false,
		}
		var err error
		res.parser, err = jsonparser.NewParserJSON(config, sniff, logger, registry)
		if err != nil {
			return nil, xerrors.Errorf("unable to create AuditTrails parser: unable to create JSON parser: %s", err)
		}
	}

	return res, nil
}
