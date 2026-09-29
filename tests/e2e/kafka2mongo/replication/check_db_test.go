package main

import (
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
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/kafka"
	_ "github.com/transferia/transferia/tests/helpers/registration/mongo"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	source = provider_kafka.KafkaSource{
		Connection: &provider_kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &provider_kafka.KafkaAuth{Enabled: false},
		Topic:            "topic1",
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     nil,
	}
	target = provider_mongo.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     testenv.GetIntFromEnv("MONGO_LOCAL_PORT"),
		Database: "db1",
		User:     os.Getenv("MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Cleanup:  model.Drop,
	}
)

func TestReplication(t *testing.T) {
	// prepare source

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

	source.ParserConfig = parserConfigMap

	// write to source topic

	k := []byte(`any_key`)
	v := []byte(`{"id": "1", "level": "my_level", "caller": "my_caller", "msg": "my_msg"}`)

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
	err = srcSink.Push([]abstract.ChangeItem{abstract.MakeRawMessage(k, source.Topic, time.Time{}, source.Topic, 0, 0, v)})
	require.NoError(t, err)

	// activate transfer

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &source, &target, abstract.TransferTypeIncrementOnly)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	require.NoError(t, storage.WaitDestinationEqualRowsCount(
		target.Database,
		"topic1",
		storagecomparison.GetSampleableStorageByModel(t, target),
		60*time.Second,
		1,
	))
}
