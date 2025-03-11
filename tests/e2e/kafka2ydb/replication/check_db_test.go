package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/parsers"
	jsonparser "github.com/transferria/transferria/pkg/parsers/registry/json"
	kafkasink "github.com/transferria/transferria/pkg/providers/kafka"
	"github.com/transferria/transferria/pkg/providers/ydb"
	"github.com/transferria/transferria/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestReplication(t *testing.T) {
	// create source
	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "level", DataType: ytschema.TypeString.String()},
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	source := &kafkasink.KafkaSource{
		Connection: &kafkasink.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafkasink.KafkaAuth{Enabled: false},
		Topic:            "topic1",
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     parserConfigMap,
		IsHomo:           false,
	}

	// create destination
	endpoint, ok := os.LookupEnv("YDB_ENDPOINT")
	if !ok {
		t.Fail()
	}
	targetPort, err := helpers.GetPortFromStr(endpoint)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YDB target", Port: targetPort},
		))
	}()

	prefix, ok := os.LookupEnv("YDB_DATABASE")
	if !ok {
		t.Fail()
	}

	token, ok := os.LookupEnv("YDB_TOKEN")
	if !ok {
		token = "anyNotEmptyString"
	}

	dst := &ydb.YdbDestination{
		Token:                 model.SecretString(token),
		Database:              prefix,
		Path:                  "",
		Instance:              endpoint,
		ShardCount:            0,
		Rotation:              nil,
		AltNames:              nil,
		Cleanup:               "",
		IsTableColumnOriented: false,
		DefaultCompression:    "off",
	}

	// write messages to source topic
	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection: source.Connection,
			Auth:       source.Auth,
			Topic:      source.Topic,
			FormatSettings: model.SerializationFormat{
				Name: model.SerializationFormatJSON,
				BatchingSettings: &model.Batching{
					Enabled:        false,
					Interval:       0,
					MaxChangeItems: 0,
					MaxMessageSize: 0,
				},
			},
			ParralelWriterCount: 10,
		},
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
	)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("%d", i))
		v := []byte(fmt.Sprintf(`{"id": "%d", "level": "my_level", "caller": "my_caller", "msg": "my_msg"}`, i))
		err = srcSink.Push([]abstract.ChangeItem{
			kafkasink.MakeKafkaRawMessage(source.Topic, time.Time{}, source.Topic, 0, 0, k, v),
		})
		require.NoError(t, err)
	}
	// activate transfer

	transfer := helpers.MakeTransfer(helpers.TransferID, source, dst, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		"",
		"topic1",
		helpers.GetSampleableStorageByModel(t, dst),
		60*time.Second,
		50,
	))
}
