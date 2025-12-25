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
	"github.com/transferia/transferia/pkg/parsers/registry/cloudevents"
	"github.com/transferia/transferia/pkg/parsers/registry/cloudevents/engine/testutils"
	"github.com/transferia/transferia/pkg/providers/kafka"
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/tests/helpers"
	confluentsrmock "github.com/transferia/transferia/tests/helpers/confluent_schema_registry_mock"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	currSource = kafka.KafkaSource{
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
	}
	target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e_replication")
)

var idToBuf = make(map[int]string)

//go:embed testdata/test_schemas.json
var jsonSchemas []byte

//go:embed testdata/topic-profile.bin
var topicProfile []byte

//go:embed testdata/topic-shot.bin
var topicShot []byte

func init() {
	var name map[string]interface{}
	_ = json.Unmarshal(jsonSchemas, &name)
	for kStr, vObj := range name {
		k, _ := strconv.Atoi(kStr)
		v, _ := json.Marshal(vObj)
		idToBuf[k] = string(v)
	}
}

func checkCase(t *testing.T, currSource *kafka.KafkaSource, topicName string, msg []byte) []abstract.ChangeItem {
	currSource.Topic = topicName

	// SR mock

	schemaRegistryMock := confluentsrmock.NewConfluentSRMock(idToBuf, nil)
	defer schemaRegistryMock.Close()

	msg = testutils.ChangeRegistryURL(t, msg, schemaRegistryMock.URL())

	// prepare currSource

	parserConfigMap, err := parsers.ParserConfigStructToMap(&cloudevents.ParserConfigCloudEventsCommon{
		SkipAuth: true,
	})
	require.NoError(t, err)
	currSource.ParserConfig = parserConfigMap

	// activate transfer

	transfer := helpers.MakeTransfer(helpers.TransferID, currSource, target, abstract.TransferTypeIncrementOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// write to currSource topic

	srcSink, err := kafka.NewReplicationSink(
		&kafka.KafkaDestination{
			Connection: currSource.Connection,
			Auth:       currSource.Auth,
			Topic:      currSource.Topic,
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
	err = srcSink.Push([]abstract.ChangeItem{abstract.MakeRawMessage([]byte("_"), currSource.Topic, time.Time{}, currSource.Topic, 0, 0, msg)})
	require.NoError(t, err)

	// check results

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", topicName, helpers.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second, 1))

	result := make([]abstract.ChangeItem, 0)
	storage, err := yt_storage.NewStorage(target.ToStorageParams())
	require.NoError(t, err)
	err = storage.LoadTable(context.Background(), abstract.TableDescription{Name: topicName}, func(input []abstract.ChangeItem) error {
		result = append(result, input...)
		return nil
	})
	require.NoError(t, err)
	return result
}

func TestReplication(t *testing.T) {
	result := make([]abstract.ChangeItem, 0)
	result = append(result, checkCase(t, &currSource, "topic-profile", topicProfile)...)
	result = append(result, checkCase(t, &currSource, "topic-shot", topicShot)...)
	for i := range result {
		result[i].CommitTime = 0
		if result[i].IsRowEvent() {
			// get back original sr uri
			uri := strings.Split(result[i].ColumnValues[3].(string), "/schemas")
			result[i].ColumnValues[3] = "http://localhost:8081/schemas" + uri[1]
			result[i].ColumnValues[5] = time.Time{} // remove 'time' from 'cloudevents' parser results
		}
	}
	canon.SaveJSON(t, result)
}
