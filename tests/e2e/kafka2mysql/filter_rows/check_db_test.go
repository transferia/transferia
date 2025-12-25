package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
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
	filterrows "github.com/transferia/transferia/pkg/transformer/registry/filter_rows"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	topicName = "testTopic"

	source = kafkasink.KafkaSource{
		Connection: &kafkasink.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafkasink.KafkaAuth{Enabled: false},
		Topic:            topicName,
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     nil,
	}
	target = *helpers.RecipeMysqlTarget()
)

func TestReplication(t *testing.T) {

	// prepare source
	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "i64", DataType: ytschema.TypeInt64.String()},
			{ColumnName: "f32", DataType: ytschema.TypeFloat32.String()},
			{ColumnName: "str", DataType: ytschema.TypeString.String()},
			{ColumnName: "date", DataType: ytschema.TypeDate.String()},
			{ColumnName: "datetime", DataType: ytschema.TypeDatetime.String()},
			{ColumnName: "time", DataType: ytschema.TypeTimestamp.String()},
			{ColumnName: "null", DataType: ytschema.TypeString.String()},
			{ColumnName: "notNull", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: false,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	source.ParserConfig = parserConfigMap

	// activate transfer
	filter := strings.Join([]string{
		"id > 1",
		"i64 < 9223372036854775807", "i64 > -9223372036854775808",
		"f32 <= 0.3",
		`str ~ "name"`, `str !~ "bad"`,
		"date > 1999-01-04", "date <= 2000-03-04",
		"datetime = 2010-01-01T00:00:00",
		"time = 2010-01-01T00:00:00",
		"null = NULL",
		"notNull != NULL",
	}, " AND ")

	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeIncrementOnly)
	transformer, err := filterrows.NewFilterRowsTransformer(
		filterrows.Config{Filter: filter},
		logger.Log,
	)
	require.NoError(t, err)
	require.NoError(t, transfer.AddExtraTransformer(transformer))
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// write to source topic
	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
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

	v1 := []byte(`{"id": "1", "i64": "9223372036854775807", "f32": "0.1", "str": "badname", "time": "2000-01-01T00:00:00", "datetime": "2000-01-01 00:00:00 +0000 UTC", "date": "1999-01-04", "null": null, "notNull": null}`)
	v2 := []byte(`{"id": "2", "i64": "200", "f32": "0.2", "str": "name", "time": "2010-01-01T00:00:00", "datetime": "2010-01-01T00:00:00 +0000 UTC", "date": "2000-03-04", "null": null, "notNull": "str"}`)
	v3 := []byte(`{"id": "3", "i64": "-9223372036854775808", "f32": "0.3", "str": "other", "time": "2005-01-01T00:00:00", "datetime": "2005-01-01T00:00:00 +0000 UTC", "date": "2000-03-05", "null": "str", "notNull": "str"}`)

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		abstract.MakeRawMessage([]byte("_"), source.Topic, time.Time{}, source.Topic, 0, 0, v1),
		abstract.MakeRawMessage([]byte("_"), source.Topic, time.Time{}, source.Topic, 0, 0, v2),
		abstract.MakeRawMessage([]byte("_"), source.Topic, time.Time{}, source.Topic, 0, 0, v3),
	}))

	// check results
	expected := []abstract.ChangeItem{{
		ColumnNames: []string{
			"id",
			"date",
			"datetime",
			"f32",
			"i64",
			"notNull",
			"null",
			"str",
			"time",
		},
		ColumnValues: []interface{}{
			int32(2),
			time.Date(2000, time.March, 4, 0, 0, 0, 0, time.UTC),
			time.Date(2010, time.January, 1, 0, 0, 0, 0, time.Local),
			json.Number("0.2"),
			int64(200),
			"str",
			nil,
			"name",
			time.Date(2010, time.January, 1, 0, 0, 0, 0, time.Local),
		},
	}}

	dst := helpers.GetSampleableStorageByModel(t, target)
	err = helpers.WaitDestinationEqualRowsCount(target.Database, topicName, dst, 300*time.Second, uint64(len(expected)))
	require.NoError(t, err)

	var actual []abstract.ChangeItem

	dst = helpers.GetSampleableStorageByModel(t, target)
	require.NoError(t, dst.LoadTable(context.Background(), abstract.TableDescription{
		Schema: target.Database,
		Name:   topicName,
	}, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.Kind != abstract.InsertKind {
				continue
			}
			item := abstract.ChangeItem{
				ColumnNames:  row.ColumnNames,
				ColumnValues: row.ColumnValues,
			}
			actual = append(actual, helpers.RemoveColumnsFromChangeItem(
				item, []string{"_idx", "_offset", "_partition", "_timestamp"}))
		}
		return nil
	}))

	require.Equal(t, expected, actual)
}
