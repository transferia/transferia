package parameters

import (
	"slices"
	"strconv"
	"strings"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/util/set"
)

// Contract for parameters: all possible keys are present

// Parameters names.
const (
	// automatically filled settings, user can override it, if user wants:

	DatabaseDBName = "database.dbname" // "db" in debezium payload - it's just field in "source". Present in pg-connector, absent in mysql-connector
	TopicPrefix    = "topic.prefix"    // "name" in debezium payload - it's prefix for topic_name & it's field "name" in "source" (used to be called "db.server.name", but was renamed)

	UnknownTypesPolicy        = "dt.unknown.types.policy" // by default, debezium skips user-defined types. We are failing by default in this case, but can just skip
	AddOriginalTypes          = "dt.add.original.type.info"
	SourceType                = "dt.source.type" // common/mysql/pg - to emit database-specific fields in 'source'
	MysqlTimeZone             = "dt.mysql.timezone"
	BatchingMaxSize           = "dt.batching.max.size"
	WriteIntoOneFullTopicName = "dt.write.into.one.topic"

	// other settings:

	TimePrecisionMode           = "time.precision.mode"
	DecimalHandlingMode         = "decimal.handling.mode"
	HstoreHandlingMode          = "hstore.handling.mode"
	IntervalHandlingMode        = "interval.handling.mode"
	TombstonesOnDelete          = "tombstones.on.delete"
	BinaryHandlingMode          = "binary.handling.mode"
	MoneyFractionDigits         = "money.fraction.digits"
	UnavailableValuePlaceholder = "unavailable.value.placeholder"

	KeyConverter                                  = "key.converter"
	KeyConverterSchemasEnable                     = "key.converter.schemas.enable"
	KeyConverterSchemaRegistryURL                 = "key.converter.schema.registry.url"
	KeyConverterBasicAuthCredentialsSource        = "key.converter.basic.auth.credentials.source"
	KeyConverterBasicAuthUserInfo                 = "key.converter.basic.auth.user.info"
	KeyConverterSslCa                             = "key.converter.ssl.ca"
	KeyConverterDTJSONGenerateClosedContentSchema = "key.converter.dt.json.generate.closed.content.schema"

	ValueConverter                                  = "value.converter"
	ValueConverterSchemasEnable                     = "value.converter.schemas.enable"
	ValueConverterSchemaRegistryURL                 = "value.converter.schema.registry.url"
	ValueConverterBasicAuthCredentialsSource        = "value.converter.basic.auth.credentials.source"
	ValueConverterBasicAuthUserInfo                 = "value.converter.basic.auth.user.info"
	ValueConverterSslCa                             = "value.converter.ssl.ca"
	ValueConverterDTJSONGenerateClosedContentSchema = "value.converter.dt.json.generate.closed.content.schema"

	KeySubjectNameStrategy   = "key.converter.key.subject.name.strategy"
	ValueSubjectNameStrategy = "value.converter.value.subject.name.strategy"
)

// Values of parameters.
const (
	BoolTrue  = "true"
	BoolFalse = "false"

	UnknownTypesPolicyFail     = "fail"
	UnknownTypesPolicySkip     = "skip"
	UnknownTypesPolicyToString = "to_string"

	SourceTypePg    = "pg"
	SourceTypeMysql = "mysql"
	SourceTypeYDB   = "ydb"

	MysqlTimeZoneUTC = "UTC"

	TimePrecisionModeAdaptive                 = "adaptive"
	TimePrecisionModeAdaptiveTimeMicroseconds = "adaptive_time_microseconds"
	TimePrecisionModeConnect                  = "connect"

	DecimalHandlingModePrecise = "precise"
	DecimalHandlingModeDouble  = "double"
	DecimalHandlingModeString  = "string"

	HstoreHandlingModeMap  = "map"
	HstoreHandlingModeJSON = "json"

	IntervalHandlingModeNumeric = "numeric"
	IntervalHandlingModeString  = "string"

	BinaryHandlingModeBytes  = "bytes"
	BinaryHandlingModeBase64 = "base64"
	BinaryHandlingModeHex    = "hex"

	ConverterApacheKafkaJSON   = "org.apache.kafka.connect.json.JsonConverter"
	ConverterConfluentAvro     = "io.confluent.connect.avro.AvroConverter"
	ConverterConfluentJSON     = "io.confluent.connect.json.JsonSchemaConverter"
	ConverterConfluentProtobuf = "io.confluent.connect.protobuf.ProtobufConverter"

	SubjectTopicNameStrategy       = "io.confluent.kafka.serializers.subject.TopicNameStrategy"
	SubjectRecordNameStrategy      = "io.confluent.kafka.serializers.subject.RecordNameStrategy"
	SubjectTopicRecordNameStrategy = "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"
)

var converterParams = set.New([]string{
	KeyConverter,
	KeyConverterSchemasEnable,
	KeyConverterSchemaRegistryURL,
	KeyConverterBasicAuthCredentialsSource,
	KeyConverterBasicAuthUserInfo,
	KeyConverterSslCa,

	ValueConverter,
	ValueConverterSchemasEnable,
	ValueConverterSchemaRegistryURL,
	ValueConverterBasicAuthCredentialsSource,
	ValueConverterBasicAuthUserInfo,
	ValueConverterSslCa,
}...)

func IsConverterParam(param string) bool {
	return converterParams.Contains(param)
}

type connectorSetting struct {
	name           string
	possibleValues []string
	defaultValue   string
}

var connectorSettings = []connectorSetting{
	{DatabaseDBName, []string{}, ""},
	{TopicPrefix, []string{}, ""},
	{UnknownTypesPolicy, []string{UnknownTypesPolicyFail, UnknownTypesPolicySkip, UnknownTypesPolicyToString}, UnknownTypesPolicyFail},
	{AddOriginalTypes, []string{BoolFalse, BoolTrue}, BoolFalse},
	{SourceType, []string{"", SourceTypePg, SourceTypeMysql, SourceTypeYDB}, ""},
	{MysqlTimeZone, []string{}, MysqlTimeZoneUTC},
	{BatchingMaxSize, []string{}, "0"},
	{WriteIntoOneFullTopicName, []string{BoolFalse, BoolTrue}, BoolFalse},

	{TimePrecisionMode, []string{TimePrecisionModeAdaptive, TimePrecisionModeAdaptiveTimeMicroseconds, TimePrecisionModeConnect}, TimePrecisionModeAdaptive},
	{DecimalHandlingMode, []string{DecimalHandlingModePrecise, DecimalHandlingModeDouble, DecimalHandlingModeString}, DecimalHandlingModePrecise}, // implemented for: pg, mysql
	{HstoreHandlingMode, []string{HstoreHandlingModeMap, HstoreHandlingModeJSON}, HstoreHandlingModeMap},
	{IntervalHandlingMode, []string{IntervalHandlingModeNumeric, IntervalHandlingModeString}, IntervalHandlingModeNumeric},
	{TombstonesOnDelete, []string{BoolTrue, BoolFalse}, BoolTrue},
	{BinaryHandlingMode, []string{BinaryHandlingModeBytes, BinaryHandlingModeBase64, BinaryHandlingModeHex}, BinaryHandlingModeBytes},
	{MoneyFractionDigits, []string{}, "2"},
	{UnavailableValuePlaceholder, []string{}, "__debezium_unavailable_value"},

	// key/value stuff

	{KeyConverter, []string{ConverterApacheKafkaJSON, ConverterConfluentJSON}, ConverterApacheKafkaJSON},
	{ValueConverter, []string{ConverterApacheKafkaJSON, ConverterConfluentJSON}, ConverterApacheKafkaJSON},

	{KeyConverterSchemasEnable, []string{BoolFalse, BoolTrue}, BoolTrue},
	{ValueConverterSchemasEnable, []string{BoolFalse, BoolTrue}, BoolTrue},

	{KeyConverterSchemaRegistryURL, []string{}, ""},
	{ValueConverterSchemaRegistryURL, []string{}, ""},

	{KeyConverterBasicAuthCredentialsSource, []string{}, ""},
	{ValueConverterBasicAuthCredentialsSource, []string{}, ""},

	{KeyConverterBasicAuthUserInfo, []string{}, ""},
	{ValueConverterBasicAuthUserInfo, []string{}, ""},

	{KeySubjectNameStrategy, []string{SubjectTopicNameStrategy, SubjectRecordNameStrategy, SubjectTopicRecordNameStrategy}, SubjectTopicNameStrategy},
	{ValueSubjectNameStrategy, []string{SubjectTopicNameStrategy, SubjectRecordNameStrategy, SubjectTopicRecordNameStrategy}, SubjectTopicNameStrategy},

	{KeyConverterSslCa, []string{}, ""},
	{ValueConverterSslCa, []string{}, ""},

	{KeyConverterDTJSONGenerateClosedContentSchema, []string{BoolFalse, BoolTrue}, BoolFalse},
	{ValueConverterDTJSONGenerateClosedContentSchema, []string{BoolFalse, BoolTrue}, BoolFalse},
}

func possibleSettingValues(settingName string) []string {
	for _, setting := range connectorSettings {
		if setting.name == settingName {
			return setting.possibleValues
		}
	}
	return nil
}

// EnrichedWithDefaults returns copy of provided params default values for parameters that are not set in the input and returns its copy.
func EnrichedWithDefaults(params map[string]string) map[string]string {
	result := make(map[string]string, len(connectorSettings))
	for _, el := range connectorSettings {
		result[el.name] = el.defaultValue
	}
	for settingName, value := range params {
		result[settingName] = value
		if _, ok := result[settingName]; !ok {
			logger.Log.Warnf("Debezium setting '%s' is not known", settingName)
			continue
		}
		possibleValues := possibleSettingValues(settingName)
		if len(possibleValues) > 0 && !slices.Contains(possibleValues, value) {
			msg := "Debezium setting '%s' value '%s' is impossible. Available values: [%s]"
			logger.Log.Errorf(msg, settingName, value, strings.Join(possibleValues, ","))
		}
	}
	return result
}

func GetDBName(in map[string]string) string {
	return in[DatabaseDBName]
}
func GetTopicPrefix(in map[string]string) string {
	return in[TopicPrefix]
}
func GetDTAddOriginalTypeInfo(in map[string]string) string {
	return in[AddOriginalTypes]
}
func GetSourceType(in map[string]string) string {
	return in[SourceType]
}
func GetMysqlTimeZone(in map[string]string) string {
	return in[MysqlTimeZone]
}
func GetBatchingMaxSize(in map[string]string) int {
	result, _ := strconv.Atoi(in[BatchingMaxSize])
	return result
}
func UseWriteIntoOneFullTopicName(in map[string]string) bool {
	return in[WriteIntoOneFullTopicName] == BoolTrue
}
func GetDecimalHandlingMode(in map[string]string) string {
	return in[DecimalHandlingMode]
}
func GetTombstonesOnDelete(in map[string]string) string {
	return in[TombstonesOnDelete]
}
func GetKeyConverter(in map[string]string) string {
	return in[KeyConverter]
}
func GetKeyConverterSchemasEnable(in map[string]string) string {
	return in[KeyConverterSchemasEnable]
}
func GetValueConverter(in map[string]string) string {
	return in[ValueConverter]
}
func GetValueConverterSchemasEnable(in map[string]string) string {
	return in[ValueConverterSchemasEnable]
}
func GetValueConverterSchemaRegistryURL(in map[string]string) string {
	return in[ValueConverterSchemaRegistryURL]
}
func GetKeyConverterSchemaRegistryURL(in map[string]string) string {
	return in[KeyConverterSchemaRegistryURL]
}
func GetValueConverterSchemaRegistryUserPassword(in map[string]string) string {
	return in[ValueConverterBasicAuthUserInfo]
}
func GetKeyConverterSchemaRegistryUserPassword(in map[string]string) string {
	return in[KeyConverterBasicAuthUserInfo]
}
func GetKeySubjectNameStrategy(in map[string]string) string {
	return in[KeySubjectNameStrategy]
}
func GetValueSubjectNameStrategy(in map[string]string) string {
	return in[ValueSubjectNameStrategy]
}
func GetKeyConverterSslCa(in map[string]string) string {
	return in[KeyConverterSslCa]
}
func GetValueConverterSslCa(in map[string]string) string {
	return in[ValueConverterSslCa]
}
func IsKeySchemaDisabled(in map[string]string) bool {
	return GetKeyConverterSchemasEnable(in) == BoolFalse
}
func IsValueSchemaDisabled(in map[string]string) bool {
	return GetValueConverterSchemasEnable(in) == BoolFalse
}
func GetKeyConverterDTJSONGenerateClosedContentSchema(in map[string]string) bool {
	return in[KeyConverterDTJSONGenerateClosedContentSchema] == BoolTrue
}
func GetValueConverterDTJSONGenerateClosedContentSchema(in map[string]string) bool {
	return in[ValueConverterDTJSONGenerateClosedContentSchema] == BoolTrue
}
