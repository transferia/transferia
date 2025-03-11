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
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	kafkasink "github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	source = kafkasink.KafkaSource{
		Connection: &kafkasink.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafkasink.KafkaAuth{Enabled: false},
		Topic:            "topic1",
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     nil,
		IsHomo:           false,
	}
	target = *helpers.RecipeMysqlTarget()
)

func TestReplication(t *testing.T) {
	// prepare source

	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "level", DataType: ytschema.TypeString.String()},
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: false,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	source.ParserConfig = parserConfigMap

	// write to source topic

	k := []byte(`any_key`)
	v := []byte(`{"id": "1", "level": "my_level", "caller": "my_caller", "msg": "my_msg"}`)

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
	err = srcSink.Push([]abstract.ChangeItem{kafkasink.MakeKafkaRawMessage(source.Topic, time.Time{}, source.Topic, 0, 0, k, v)})
	require.NoError(t, err)

	// activate transfer

	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		"topic1",
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		1,
	))
}
