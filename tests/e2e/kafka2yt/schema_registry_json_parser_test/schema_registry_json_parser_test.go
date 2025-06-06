package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry"
	"github.com/transferia/transferia/pkg/providers/kafka"
	ytStorage "github.com/transferia/transferia/pkg/providers/yt/storage"
	replaceprimarykey "github.com/transferia/transferia/pkg/transformer/registry/replace_primary_key"
	"github.com/transferia/transferia/tests/helpers"
	confluentsrmock "github.com/transferia/transferia/tests/helpers/confluent_schema_registry_mock"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	currSource = &kafka.KafkaSource{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafka.KafkaAuth{Enabled: false},
		Topic:            "",
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     nil,
		IsHomo:           false,
	}
	target = yt_helpers.RecipeYtTarget("//home/confluent_sr/test/kafka2yt_e2e_replication")
)

var idToBuf = make(map[int]string)

//go:embed testdata/test_schemas.json
var jsonSchemas []byte

//go:embed testdata/test_messages.bin
var messages []byte

func init() {
	var name map[string]interface{}
	_ = json.Unmarshal(jsonSchemas, &name)
	for kStr, vObj := range name {
		k, _ := strconv.Atoi(kStr)
		v, _ := json.Marshal(vObj)
		idToBuf[k] = string(v)
	}
}

func TestSchemaRegistryJSONtoYT(t *testing.T) {
	const topicName = "testTopic"

	// SR mock
	schemaRegistryMock := confluentsrmock.NewConfluentSRMock(idToBuf, nil)
	defer schemaRegistryMock.Close()

	// prepare currSource
	parserConfigMap, err := parsers.ParserConfigStructToMap(&confluentschemaregistry.ParserConfigConfluentSchemaRegistryCommon{
		SchemaRegistryURL: schemaRegistryMock.URL(),
		SkipAuth:          true,
	})
	require.NoError(t, err)
	currSource.ParserConfig = parserConfigMap
	currSource.Topic = topicName

	// add transformation and activate transfer
	transfer := helpers.MakeTransfer(helpers.TransferID, currSource, target, abstract.TransferTypeIncrementOnly)
	transformer, err := replaceprimarykey.NewReplacePrimaryKeyTransformer(replaceprimarykey.Config{
		Keys: []string{"id"},
	})
	require.NoError(t, err)
	require.NoError(t, transfer.AddExtraTransformer(transformer))
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// write to currSource topic
	srcSink, err := kafka.NewReplicationSink(
		&kafka.KafkaDestination{
			Connection: currSource.Connection,
			Auth:       currSource.Auth,
			Topic:      currSource.Topic,
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
	for _, message := range strings.Split(string(messages), "\n") {
		err = srcSink.Push(
			[]abstract.ChangeItem{kafka.MakeKafkaRawMessage(currSource.Topic, time.Time{}, currSource.Topic, 0, 0, []byte("_"), []byte(message))})
		require.NoError(t, err)
	}

	// check results
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("schema", "table", helpers.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second, 2))
	result := make([]abstract.ChangeItem, 0)
	storage, err := ytStorage.NewStorage(target.ToStorageParams())
	require.NoError(t, err)
	err = storage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "table",
		Schema: "schema",
	}, func(input []abstract.ChangeItem) error {
		result = append(result, input...)
		return nil
	})
	require.NoError(t, err)
	for i := range result {
		result[i].CommitTime = 0
	}
	canon.SaveJSON(t, result)
}
