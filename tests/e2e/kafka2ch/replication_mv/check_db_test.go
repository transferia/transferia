package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	parser_json "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_kafka "github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/transformer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	kafkaTopic = "topic1"
	source     = *provider_kafka.MustSourceRecipe()

	chDatabase = "public"
	target     = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(chDatabase))

	timestampToUse = time.Date(2024, 03, 19, 0, 0, 0, 0, time.Local)
)

func includeAllTables(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

func fixTimestampMiddleware(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
	for _, item := range items {
		for i, name := range item.ColumnNames {
			if name == "_timestamp" {
				// Fix timestamp to support canonization
				item.ColumnValues[i] = timestampToUse
				break
			}
		}
	}

	return abstract.TransformerResult{
		Transformed: items,
	}
}

func TestReplication(t *testing.T) {
	// prepare source

	target.Cleanup = model.DisabledCleanup
	target.InsertParams = clickhouse_model.InsertParams{MaterializedViewsIgnoreErrors: true}

	parserConfigStruct := &parser_json.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "level", DataType: ytschema.TypeString.String()},
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:         false,
		NullKeysAllowed: true, // ID can be null, but mat-view expect it not nullable
		AddDedupeKeys:   true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	source.ParserConfig = parserConfigMap
	source.Topic = kafkaTopic

	// write to source topic

	srcSink, err := provider_kafka.NewReplicationSink(
		&provider_kafka.KafkaDestination{
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
	err = srcSink.Push([]abstract.ChangeItem{
		abstract.MakeRawMessage(
			[]byte(`any_key_2`),
			source.Topic,
			time.Time{},
			source.Topic,
			0,
			1,
			[]byte(`{"level": "my_level", "caller": "my_caller", "msg": "my_msg"}`), // no ID column, should fail matview.
		),
	})
	require.NoError(t, err)

	// activate transfer

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &source, &target, abstract.TransferTypeIncrementOnly)
	// add transformation in order to control Kafka timestamp
	err = transfer.AddExtraTransformer(transformer.NewSimpleTransformer(t, fixTimestampMiddleware, includeAllTables))
	require.NoError(t, err)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	require.NoError(t, storage.WaitDestinationEqualRowsCount(
		target.Database,
		kafkaTopic,
		storagecomparison.GetSampleableStorageByModel(t, target),
		60*time.Second,
		1,
	))
}
