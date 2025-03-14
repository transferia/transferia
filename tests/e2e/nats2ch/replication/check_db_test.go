package main

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/nats/connection"
	"os"
	"testing"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	natsrecipe "github.com/transferia/transferia/pkg/providers/nats/recipe"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	natsTestUrl = "NATS_TEST_URL"
)

func TestReplicationSingleStreamSingleSubject(t *testing.T) {
	var (
		transferType = abstract.TransferTypeIncrementOnly
		chDatabase   = "public"
		natsSource   = *natsrecipe.MustSource(natsrecipe.WithStreams([]*nats.StreamConfig{{
			Name:       "test_stream",
			Subjects:   []string{"test.subject"},
			Retention:  nats.LimitsPolicy,
			MaxMsgs:    -1,
			Discard:    nats.DiscardOld,
			MaxAge:     1 * time.Hour,
			Storage:    nats.FileStorage,
			Duplicates: 2 * time.Minute,
		}}))
		target = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(chDatabase))
	)

	natsSource.WithStreamIngestionConfig(getStreamIngestionConfigSingleStreamSingleSubject(t))

	// publishing messages to single subject
	err := publishMessagesSingleStreamSingleSubject()
	require.NoError(t, err)

	// activate transfer
	transfer := helpers.MakeTransfer(helpers.TransferID, &natsSource, &target, transferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// check results
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		natsSource.Config.StreamIngestionConfigs[0].SubjectIngestionConfigs[0].TableName,
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		10,
	))
}

func getStreamIngestionConfigSingleStreamSingleSubject(t *testing.T) []*connection.StreamIngestionConfig {
	streamIngestionConfig := []*connection.StreamIngestionConfig{
		{
			Stream: "test_stream",
			SubjectIngestionConfigs: []*connection.SubjectIngestionConfig{
				{
					TableName: "nats_test_stream_ingestion",
					ConsumerConfig: &jetstream.ConsumerConfig{
						Durable:         "test_consumer",
						Name:            "test_consumer",
						DeliverPolicy:   jetstream.DeliverAllPolicy,
						AckPolicy:       jetstream.AckAllPolicy,
						FilterSubjects:  []string{"test.subject"},
						MaxRequestBatch: 100,
					},
				},
			},
		},
	}

	parserConfig1 := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "name", DataType: ytschema.TypeString.String()},
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: false,
	}

	parserConfigMap1, err := parsers.ParserConfigStructToMap(parserConfig1)
	require.NoError(t, err)

	streamIngestionConfig[0].SubjectIngestionConfigs[0].ParserConfig = parserConfigMap1
	return streamIngestionConfig
}

func TestReplicationMulti(t *testing.T) {
	var (
		transferType = abstract.TransferTypeIncrementOnly
		chDatabase   = "public"
		target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(chDatabase))
		natsSource   = *natsrecipe.MustSource(natsrecipe.WithStreams([]*nats.StreamConfig{
			{
				Name:       "test_stream",
				Subjects:   []string{"test.subject", "test.subject.2"},
				Retention:  nats.LimitsPolicy,
				MaxMsgs:    -1,
				Discard:    nats.DiscardOld,
				MaxAge:     1 * time.Hour,
				Storage:    nats.FileStorage,
				Duplicates: 2 * time.Minute,
			},
			{
				Name:       "test_stream_2",
				Subjects:   []string{"test2.subject"},
				Retention:  nats.LimitsPolicy,
				MaxMsgs:    -1,
				Discard:    nats.DiscardOld,
				MaxAge:     1 * time.Hour,
				Storage:    nats.FileStorage,
				Duplicates: 2 * time.Minute,
			},
		}))
	)

	natsSource.WithStreamIngestionConfig(getStreamIngestionConfigMulti(t))

	// publishing messages to single subject
	err := publishMessagesMultipleStreamMultipleSubject()
	require.NoError(t, err)

	// activate transfer
	transfer := helpers.MakeTransfer(helpers.TransferID, &natsSource, &target, transferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	// Subject: test.subject, messages published: 10
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		natsSource.Config.StreamIngestionConfigs[0].SubjectIngestionConfigs[0].TableName,
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		10,
	))

	// Subject: test.subject.2, messages published: 10
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		natsSource.Config.StreamIngestionConfigs[0].SubjectIngestionConfigs[1].TableName,
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		10,
	))

	// Subject: test2.subject, messages published: 10
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		natsSource.Config.StreamIngestionConfigs[1].SubjectIngestionConfigs[0].TableName,
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		10,
	))

}

func getStreamIngestionConfigMulti(t *testing.T) []*connection.StreamIngestionConfig {
	streamIngestionConfig := []*connection.StreamIngestionConfig{
		{
			Stream: "test_stream",
			SubjectIngestionConfigs: []*connection.SubjectIngestionConfig{
				{
					TableName: "nats_test_stream_ingestion",
					ConsumerConfig: &jetstream.ConsumerConfig{
						Durable:         "test_consumer",
						Name:            "test_consumer",
						DeliverPolicy:   jetstream.DeliverAllPolicy,
						AckPolicy:       jetstream.AckAllPolicy,
						FilterSubject:   "test.subject",
						MaxRequestBatch: 200,
					},
				},
				{
					TableName: "nats_test_stream_ingestion_2",
					ConsumerConfig: &jetstream.ConsumerConfig{
						Durable:         "test_consumer_2",
						Name:            "test_consumer_2",
						DeliverPolicy:   jetstream.DeliverAllPolicy,
						AckPolicy:       jetstream.AckAllPolicy,
						FilterSubject:   "test.subject.2",
						MaxRequestBatch: 200,
					},
				},
			},
		},
		{
			Stream: "test_stream_2",
			SubjectIngestionConfigs: []*connection.SubjectIngestionConfig{
				{
					TableName: "nats_test_stream_2_ingestion",
					ConsumerConfig: &jetstream.ConsumerConfig{
						Durable:         "test_2_consumer",
						Name:            "test_2_consumer",
						DeliverPolicy:   jetstream.DeliverAllPolicy,
						AckPolicy:       jetstream.AckAllPolicy,
						FilterSubject:   "test2.subject",
						MaxRequestBatch: 200,
					},
				},
			},
		},
	}

	parserConfig1 := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "name", DataType: ytschema.TypeString.String()},
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: false,
	}

	parserConfigMap1, err := parsers.ParserConfigStructToMap(parserConfig1)
	require.NoError(t, err)

	streamIngestionConfig[0].SubjectIngestionConfigs[0].ParserConfig = parserConfigMap1

	parserConfig2 := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "name", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: false,
	}

	parserConfigMap2, err := parsers.ParserConfigStructToMap(parserConfig2)
	require.NoError(t, err)

	streamIngestionConfig[0].SubjectIngestionConfigs[1].ParserConfig = parserConfigMap2

	parserConfig3 := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "key", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "value", DataType: ytschema.TypeString.String()},
			{ColumnName: "verbose", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: false,
	}

	parserConfigMap3, err := parsers.ParserConfigStructToMap(parserConfig3)
	require.NoError(t, err)

	streamIngestionConfig[1].SubjectIngestionConfigs[0].ParserConfig = parserConfigMap3

	return streamIngestionConfig
}

// publishsing messages for testcases

func publishMessagesSingleStreamSingleSubject() error {
	return publishMessages("test.subject", [][]byte{
		[]byte(`{"id":1,"name":"Alice1","caller":"12345","msg":"Hello1"}`),
		[]byte(`{"id":2,"name":"Alice2","caller":"12345","msg":"Hello2"}`),
		[]byte(`{"id":3,"name":"Alice3","caller":"12345","msg":"Hello3"}`),
		[]byte(`{"id":4,"name":"Alice4","caller":"12345","msg":"Hello4"}`),
		[]byte(`{"id":5,"name":"Alice5","caller":"12345","msg":"Hello5"}`),
		[]byte(`{"id":6,"name":"Alice6","caller":"12345","msg":"Hello6"}`),
		[]byte(`{"id":7,"name":"Alice7","caller":"12345","msg":"Hello7"}`),
		[]byte(`{"id":8,"name":"Alice8","caller":"12345","msg":"Hello8"}`),
		[]byte(`{"id":9,"name":"Alice9","caller":"12345","msg":"Hello9"}`),
		[]byte(`{"id":10,"name":"Alice10","caller":"12345","msg":"Hello10"}`),
	})
}

func publishMessagesMultipleStreamMultipleSubject() error {
	// stream 1, subject 1
	err := publishMessages("test.subject", [][]byte{
		[]byte(`{"id":1,"name":"Alice1","caller":"12345","msg":"Hello1"}`),
		[]byte(`{"id":2,"name":"Alice2","caller":"12345","msg":"Hello2"}`),
		[]byte(`{"id":3,"name":"Alice3","caller":"12345","msg":"Hello3"}`),
		[]byte(`{"id":4,"name":"Alice4","caller":"12345","msg":"Hello4"}`),
		[]byte(`{"id":5,"name":"Alice5","caller":"12345","msg":"Hello5"}`),
		[]byte(`{"id":6,"name":"Alice6","caller":"12345","msg":"Hello6"}`),
		[]byte(`{"id":7,"name":"Alice7","caller":"12345","msg":"Hello7"}`),
		[]byte(`{"id":8,"name":"Alice8","caller":"12345","msg":"Hello8"}`),
		[]byte(`{"id":9,"name":"Alice9","caller":"12345","msg":"Hello9"}`),
		[]byte(`{"id":10,"name":"Alice10","caller":"12345","msg":"Hello10"}`),
	})

	if err != nil {
		return err
	}

	// stream 1, subject 2
	err = publishMessages("test.subject.2", [][]byte{
		[]byte(`{"id":1,"name":"Alice1","msg":"Hello1"}`),
		[]byte(`{"id":2,"name":"Alice2","msg":"Hello2"}`),
		[]byte(`{"id":3,"name":"Alice3","msg":"Hello3"}`),
		[]byte(`{"id":4,"name":"Alice4","msg":"Hello4"}`),
		[]byte(`{"id":5,"name":"Alice5","msg":"Hello5"}`),
		[]byte(`{"id":6,"name":"Alice6","msg":"Hello6"}`),
		[]byte(`{"id":7,"name":"Alice7","msg":"Hello7"}`),
		[]byte(`{"id":8,"name":"Alice8","msg":"Hello8"}`),
		[]byte(`{"id":9,"name":"Alice9","msg":"Hello9"}`),
		[]byte(`{"id":10,"name":"Alice10","msg":"Hello10"}`),
	})

	if err != nil {
		return err
	}

	// stream 2, subject 1
	return publishMessages("test2.subject", [][]byte{
		[]byte(`{"key":1,"value":"foo","verbose":"true"}`),
		[]byte(`{"key":2,"value":"bar","verbose":"false"}`),
		[]byte(`{"key":3,"value":"baz","verbose":"true"}`),
		[]byte(`{"key":4,"value":"foo","verbose":"true"}`),
		[]byte(`{"key":5,"value":"bar","verbose":"false"}`),
		[]byte(`{"key":6,"value":"baz","verbose":"true"}`),
		[]byte(`{"key":7,"value":"foo","verbose":"true"}`),
		[]byte(`{"key":8,"value":"bar","verbose":"false"}`),
		[]byte(`{"key":9,"value":"baz","verbose":"true"}`),
		[]byte(`{"key":10,"value":"baz","verbose":"true"}`),
	})

}

func publishMessages(subject string, messages [][]byte) error {
	url := nats.DefaultURL
	if natsrecipe.ContainerNeeded() {
		url = os.Getenv(natsTestUrl)
	}
	natsConn, err := nats.Connect(url)
	defer natsConn.Close()
	if err != nil {
		return err
	}

	for _, msg := range messages {
		err = natsConn.Publish(subject, msg)
		if err != nil {
			return err
		}
	}
	return nil
}
