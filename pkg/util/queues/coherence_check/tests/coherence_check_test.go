package tests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/debezium"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/providers/airbyte"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/eventhub"
	"github.com/transferia/transferia/pkg/providers/greenplum"
	"github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/providers/logbroker"
	"github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/oracle"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/ydb"
	ydssource "github.com/transferia/transferia/pkg/providers/yds/source"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/util/queues/coherence_check"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func parserJSONCommon(t *testing.T) map[string]interface{} {
	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	return parserConfigMap
}

func parserDebeziumCommon(t *testing.T) map[string]interface{} {
	parserConfigStruct := &debezium.ParserConfigDebeziumCommon{}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	return parserConfigMap
}

func checkDst(t *testing.T, src dp_model.Source, serializerName dp_model.SerializationFormatName, transferType abstract.TransferType, expectedOk bool) {
	dst := kafka.KafkaDestination{FormatSettings: dp_model.SerializationFormat{Name: serializerName}}
	if expectedOk {
		require.NoError(t, dst.Compatible(src, transferType))
	} else {
		require.Error(t, dst.Compatible(src, transferType))
	}
}

func TestSourceCompatible(t *testing.T) {
	type testCase struct {
		src                         dp_model.Source
		serializationFormat         dp_model.SerializationFormatName
		expectedOk                  bool
		inferredSerializationFormat dp_model.SerializationFormatName
	}

	testCases := []testCase{
		{&logbroker.LfSource{ParserConfig: nil}, dp_model.SerializationFormatMirror, false, ""},
		{&logbroker.LfSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatMirror, false, ""},
		{&logbroker.LfSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatMirror, false, ""},

		{&logbroker.LfSource{ParserConfig: nil}, dp_model.SerializationFormatJSON, false, ""},
		{&logbroker.LfSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatJSON, true, dp_model.SerializationFormatJSON},
		{&logbroker.LfSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatJSON, false, ""},

		{&logbroker.LfSource{ParserConfig: nil}, dp_model.SerializationFormatDebezium, false, ""},
		{&logbroker.LfSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatDebezium, false, ""},
		{&logbroker.LfSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatDebezium, false, ""},

		{&kafka.KafkaSource{ParserConfig: nil}, dp_model.SerializationFormatMirror, true, dp_model.SerializationFormatMirror},
		{&kafka.KafkaSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatMirror, false, ""},
		{&kafka.KafkaSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatMirror, false, ""},

		{&kafka.KafkaSource{ParserConfig: nil}, dp_model.SerializationFormatJSON, false, ""},
		{&kafka.KafkaSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatJSON, true, dp_model.SerializationFormatJSON},
		{&kafka.KafkaSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatJSON, false, ""},

		{&kafka.KafkaSource{ParserConfig: nil}, dp_model.SerializationFormatDebezium, false, ""},
		{&kafka.KafkaSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatDebezium, false, ""},
		{&kafka.KafkaSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatDebezium, false, ""},

		{&eventhub.EventHubSource{ParserConfig: nil}, dp_model.SerializationFormatMirror, true, dp_model.SerializationFormatMirror},
		{&eventhub.EventHubSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatMirror, false, ""},
		{&eventhub.EventHubSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatMirror, false, ""},

		{&eventhub.EventHubSource{ParserConfig: nil}, dp_model.SerializationFormatJSON, false, ""},
		{&eventhub.EventHubSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatJSON, true, dp_model.SerializationFormatJSON},
		{&eventhub.EventHubSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatJSON, false, ""},

		{&eventhub.EventHubSource{ParserConfig: nil}, dp_model.SerializationFormatDebezium, false, ""},
		{&eventhub.EventHubSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatDebezium, false, ""},
		{&eventhub.EventHubSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatDebezium, false, ""},

		{&ydssource.YDSSource{ParserConfig: nil}, dp_model.SerializationFormatMirror, true, dp_model.SerializationFormatMirror},
		{&ydssource.YDSSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatMirror, false, ""},
		{&ydssource.YDSSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatMirror, false, ""},

		{&ydssource.YDSSource{ParserConfig: nil}, dp_model.SerializationFormatJSON, false, ""},
		{&ydssource.YDSSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatJSON, true, dp_model.SerializationFormatJSON},
		{&ydssource.YDSSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatJSON, false, ""},

		{&ydssource.YDSSource{ParserConfig: nil}, dp_model.SerializationFormatDebezium, false, ""},
		{&ydssource.YDSSource{ParserConfig: parserJSONCommon(t)}, dp_model.SerializationFormatDebezium, false, ""},
		{&ydssource.YDSSource{ParserConfig: parserDebeziumCommon(t)}, dp_model.SerializationFormatDebezium, false, ""},

		{&postgres.PgSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&postgres.PgSource{}, dp_model.SerializationFormatJSON, false, ""},
		{&postgres.PgSource{}, dp_model.SerializationFormatDebezium, true, dp_model.SerializationFormatDebezium},

		{&mysql.MysqlSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&mysql.MysqlSource{}, dp_model.SerializationFormatJSON, false, ""},
		{&mysql.MysqlSource{}, dp_model.SerializationFormatDebezium, true, dp_model.SerializationFormatDebezium},

		{&ydb.YdbSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&ydb.YdbSource{}, dp_model.SerializationFormatJSON, false, ""},
		{&ydb.YdbSource{}, dp_model.SerializationFormatDebezium, true, dp_model.SerializationFormatDebezium},

		{&airbyte.AirbyteSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&airbyte.AirbyteSource{}, dp_model.SerializationFormatJSON, true, dp_model.SerializationFormatJSON},
		{&airbyte.AirbyteSource{}, dp_model.SerializationFormatDebezium, false, ""},

		{&model.ChSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&model.ChSource{}, dp_model.SerializationFormatJSON, false, ""},
		{&model.ChSource{}, dp_model.SerializationFormatDebezium, false, ""},

		{&greenplum.GpSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&greenplum.GpSource{}, dp_model.SerializationFormatJSON, false, ""},
		{&greenplum.GpSource{}, dp_model.SerializationFormatDebezium, false, ""},

		{&mongo.MongoSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&mongo.MongoSource{}, dp_model.SerializationFormatJSON, false, ""},
		{&mongo.MongoSource{}, dp_model.SerializationFormatDebezium, false, ""},

		{&oracle.OracleSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&oracle.OracleSource{}, dp_model.SerializationFormatJSON, false, ""},
		{&oracle.OracleSource{}, dp_model.SerializationFormatDebezium, false, ""},

		{&yt.YtSource{}, dp_model.SerializationFormatMirror, false, ""},
		{&yt.YtSource{}, dp_model.SerializationFormatJSON, false, ""},
		{&yt.YtSource{}, dp_model.SerializationFormatDebezium, false, ""},
	}

	for i, el := range testCases {
		fmt.Println(i)
		checkDst(t, el.src, el.serializationFormat, abstract.TransferTypeIncrementOnly, el.expectedOk)
		if el.expectedOk {
			require.Equal(t, el.inferredSerializationFormat, coherence_check.InferFormatSettings(el.src, dp_model.SerializationFormat{Name: dp_model.SerializationFormatAuto}).Name)
		} else {
			require.Equal(t, string(el.inferredSerializationFormat), "")
		}
	}
}
