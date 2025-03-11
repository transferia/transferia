package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	dp_model "github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/parsers"
	jsonparser "github.com/transferria/transferria/pkg/parsers/registry/json"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	kafkasink "github.com/transferria/transferria/pkg/providers/kafka"
	"github.com/transferria/transferria/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	kafkaTopic = "topic1"
	source     = *kafkasink.MustSourceRecipe()

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

	target.Cleanup = dp_model.DisabledCleanup
	target.InsertParams = model.InsertParams{MaterializedViewsIgnoreErrors: true}

	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
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

	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection: source.Connection,
			Auth:       source.Auth,
			Topic:      source.Topic,
			FormatSettings: dp_model.SerializationFormat{
				Name: dp_model.SerializationFormatJSON,
				BatchingSettings: &dp_model.Batching{
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
		kafkasink.MakeKafkaRawMessage(
			source.Topic,
			time.Time{},
			source.Topic,
			0,
			1,
			[]byte(`any_key_2`),
			[]byte(`{"level": "my_level", "caller": "my_caller", "msg": "my_msg"}`), // no ID column, should fail matview.
		),
	})
	require.NoError(t, err)

	// activate transfer

	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeIncrementOnly)
	// add transformation in order to control Kafka timestamp
	err = transfer.AddExtraTransformer(helpers.NewSimpleTransformer(t, fixTimestampMiddleware, includeAllTables))
	require.NoError(t, err)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		kafkaTopic,
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		1,
	))
}
