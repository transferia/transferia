package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	parser_json "github.com/transferia/transferia/pkg/parsers/registry/json"
	provider_kafka "github.com/transferia/transferia/pkg/providers/kafka"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestReplication(t *testing.T) {
	// create source
	parserConfigStruct := &parser_json.ParserConfigJSONCommon{
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

	source := &provider_kafka.KafkaSource{
		Connection: &provider_kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &provider_kafka.KafkaAuth{Enabled: false},
		Topic:            "topic1",
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     parserConfigMap,
	}

	// create destination
	endpoint, ok := os.LookupEnv("YDB_ENDPOINT")
	if !ok {
		t.Fail()
	}
	targetPort, err := network.GetPortFromStr(endpoint)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "YDB target", Port: targetPort},
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

	dst := &provider_ydb.YdbDestination{
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
	srcSink, err := provider_kafka.NewReplicationSink(
		&provider_kafka.KafkaDestination{
			Connection: source.Connection,
			Auth:       source.Auth,
			Topic:      source.Topic,
			FormatSettings: model.SerializationFormat{
				Name: model.SerializationFormatMirror,
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
			abstract.MakeRawMessage(k, source.Topic, time.Time{}, source.Topic, 0, 0, v),
		})
		require.NoError(t, err)
	}
	// activate transfer

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, dst, abstract.TransferTypeIncrementOnly)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	require.NoError(t, storage.WaitDestinationEqualRowsCount(
		"",
		"topic1",
		storagecomparison.GetSampleableStorageByModel(t, dst),
		60*time.Second,
		50,
	))
}
