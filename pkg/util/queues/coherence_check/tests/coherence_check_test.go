package tests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/parsers"
	parser_debezium "github.com/transferia/transferia/pkg/parsers/registry/debezium"
	parser_json "github.com/transferia/transferia/pkg/parsers/registry/json"
	provider_airbyte "github.com/transferia/transferia/pkg/providers/airbyte"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_eventhub "github.com/transferia/transferia/pkg/providers/eventhub"
	provider_greenplum "github.com/transferia/transferia/pkg/providers/greenplum"
	provider_kafka "github.com/transferia/transferia/pkg/providers/kafka"
	provider_logbroker "github.com/transferia/transferia/pkg/providers/logbroker"
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	yds_source "github.com/transferia/transferia/pkg/providers/yds/source"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/util/queues/coherence_check"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func parserJSONCommon(t *testing.T) map[string]interface{} {
	parserConfigStruct := &parser_json.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	return parserConfigMap
}

func parserDebeziumCommon(t *testing.T) map[string]interface{} {
	parserConfigStruct := &parser_debezium.ParserConfigDebeziumCommon{}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	return parserConfigMap
}

func checkDst(t *testing.T, src model.Source, serializerName model.SerializationFormatName, transferType abstract.TransferType, expectedOk bool) {
	dst := provider_kafka.KafkaDestination{FormatSettings: model.SerializationFormat{Name: serializerName}}
	if expectedOk {
		require.NoError(t, dst.Compatible(src, transferType))
	} else {
		require.Error(t, dst.Compatible(src, transferType))
	}
}

func TestSourceCompatible(t *testing.T) {
	// Логика какая
	//     - src - задает источник
	//     - serializationFormat - что будет выбрано в UI
	//     - expectedOk - позволит ли создать или нет
	//     - inferredSerializationFormat - если настроено auto, то что должно автовывестись
	type testCase struct {
		src                         model.Source
		serializationFormat         model.SerializationFormatName
		expectedOk                  bool
		inferredSerializationFormat model.SerializationFormatName
	}

	testCases := []testCase{
		{&provider_logbroker.LfSource{ParserConfig: nil}, model.SerializationFormatJSON, false, ""},
		{&provider_logbroker.LfSource{ParserConfig: parserJSONCommon(t)}, model.SerializationFormatJSON, true, model.SerializationFormatJSON},
		{&provider_logbroker.LfSource{ParserConfig: parserDebeziumCommon(t)}, model.SerializationFormatJSON, false, ""},

		{&provider_logbroker.LfSource{ParserConfig: nil}, model.SerializationFormatDebezium, false, ""},
		{&provider_logbroker.LfSource{ParserConfig: parserJSONCommon(t)}, model.SerializationFormatDebezium, false, model.SerializationFormatJSON},
		{&provider_logbroker.LfSource{ParserConfig: parserDebeziumCommon(t)}, model.SerializationFormatDebezium, false, ""},

		{&provider_kafka.KafkaSource{ParserConfig: nil}, model.SerializationFormatJSON, false, model.SerializationFormatMirror},
		{&provider_kafka.KafkaSource{ParserConfig: parserJSONCommon(t)}, model.SerializationFormatJSON, true, model.SerializationFormatJSON},
		{&provider_kafka.KafkaSource{ParserConfig: parserDebeziumCommon(t)}, model.SerializationFormatJSON, false, ""},

		{&provider_kafka.KafkaSource{ParserConfig: nil}, model.SerializationFormatDebezium, false, model.SerializationFormatMirror},
		{&provider_kafka.KafkaSource{ParserConfig: parserJSONCommon(t)}, model.SerializationFormatDebezium, false, model.SerializationFormatJSON},
		{&provider_kafka.KafkaSource{ParserConfig: parserDebeziumCommon(t)}, model.SerializationFormatDebezium, false, ""},

		{&provider_eventhub.EventHubSource{ParserConfig: nil}, model.SerializationFormatJSON, false, model.SerializationFormatMirror},
		{&provider_eventhub.EventHubSource{ParserConfig: parserJSONCommon(t)}, model.SerializationFormatJSON, true, model.SerializationFormatJSON},
		{&provider_eventhub.EventHubSource{ParserConfig: parserDebeziumCommon(t)}, model.SerializationFormatJSON, false, ""},

		{&provider_eventhub.EventHubSource{ParserConfig: nil}, model.SerializationFormatDebezium, false, model.SerializationFormatMirror},
		{&provider_eventhub.EventHubSource{ParserConfig: parserJSONCommon(t)}, model.SerializationFormatDebezium, false, model.SerializationFormatJSON},
		{&provider_eventhub.EventHubSource{ParserConfig: parserDebeziumCommon(t)}, model.SerializationFormatDebezium, false, ""},

		{&yds_source.YDSSource{ParserConfig: nil}, model.SerializationFormatJSON, false, model.SerializationFormatMirror},
		{&yds_source.YDSSource{ParserConfig: parserJSONCommon(t)}, model.SerializationFormatJSON, true, model.SerializationFormatJSON},
		{&yds_source.YDSSource{ParserConfig: parserDebeziumCommon(t)}, model.SerializationFormatJSON, false, ""},

		{&yds_source.YDSSource{ParserConfig: nil}, model.SerializationFormatDebezium, false, model.SerializationFormatMirror},
		{&yds_source.YDSSource{ParserConfig: parserJSONCommon(t)}, model.SerializationFormatDebezium, false, model.SerializationFormatJSON},
		{&yds_source.YDSSource{ParserConfig: parserDebeziumCommon(t)}, model.SerializationFormatDebezium, false, ""},

		{&provider_postgres.PgSource{}, model.SerializationFormatJSON, false, model.SerializationFormatDebezium},
		{&provider_postgres.PgSource{}, model.SerializationFormatDebezium, true, model.SerializationFormatDebezium},

		{&provider_mysql.MysqlSource{}, model.SerializationFormatJSON, false, model.SerializationFormatDebezium},
		{&provider_mysql.MysqlSource{}, model.SerializationFormatDebezium, true, model.SerializationFormatDebezium},

		{&provider_ydb.YdbSource{}, model.SerializationFormatJSON, false, model.SerializationFormatDebezium},
		{&provider_ydb.YdbSource{}, model.SerializationFormatDebezium, true, model.SerializationFormatDebezium},

		{&provider_airbyte.AirbyteSource{}, model.SerializationFormatJSON, true, model.SerializationFormatJSON},
		{&provider_airbyte.AirbyteSource{}, model.SerializationFormatDebezium, false, model.SerializationFormatJSON},

		{&clickhouse_model.ChSource{}, model.SerializationFormatJSON, false, model.SerializationFormatNative},
		{&clickhouse_model.ChSource{}, model.SerializationFormatDebezium, false, model.SerializationFormatNative},

		{&provider_greenplum.GpSource{}, model.SerializationFormatJSON, false, ""},
		{&provider_greenplum.GpSource{}, model.SerializationFormatDebezium, false, ""},

		{&provider_mongo.MongoSource{}, model.SerializationFormatJSON, false, ""},
		{&provider_mongo.MongoSource{}, model.SerializationFormatDebezium, false, ""},

		{&provider_oracle.OracleSource{}, model.SerializationFormatJSON, false, ""},
		{&provider_oracle.OracleSource{}, model.SerializationFormatDebezium, false, ""},

		{&provider_yt.YtSource{}, model.SerializationFormatJSON, false, ""},
		{&provider_yt.YtSource{}, model.SerializationFormatDebezium, false, ""},
	}

	for i, el := range testCases {
		fmt.Println(i)
		require.True(t, el.serializationFormat == model.SerializationFormatJSON || el.serializationFormat == model.SerializationFormatDebezium)
		checkDst(t, el.src, el.serializationFormat, abstract.TransferTypeIncrementOnly, el.expectedOk)
		result, err := coherence_check.InferFormatSettings(logger.Log, el.src, model.SerializationFormat{Name: model.SerializationFormatAuto})
		if err == nil {
			require.Equal(t, el.inferredSerializationFormat, result.Name)
		}
	}
}

func TestAutoFormatFillsSourceType(t *testing.T) {
	format, err := coherence_check.InferFormatSettings(logger.Log, &provider_postgres.PgSource{}, model.SerializationFormat{Name: model.SerializationFormatAuto})
	require.NoError(t, err)
	require.Equal(t, model.SerializationFormatDebezium, format.Name)
	require.Equal(t, "pg", format.Settings[debezium_parameters.SourceType])

	format2, err := coherence_check.InferFormatSettings(logger.Log, &provider_mysql.MysqlSource{}, model.SerializationFormat{Name: model.SerializationFormatAuto})
	require.NoError(t, err)
	require.Equal(t, model.SerializationFormatDebezium, format2.Name)
	require.Equal(t, "mysql", format2.Settings[debezium_parameters.SourceType])

	format3, err := coherence_check.InferFormatSettings(logger.Log, &provider_ydb.YdbSource{}, model.SerializationFormat{Name: model.SerializationFormatAuto})
	require.NoError(t, err)
	require.Equal(t, model.SerializationFormatDebezium, format3.Name)
	require.Equal(t, "ydb", format3.Settings[debezium_parameters.SourceType])
}
