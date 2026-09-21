package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	parser_json "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/providers/kafka"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	_ "github.com/transferia/transferia/pkg/providers/s3/provider"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers/s3"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap/zapcore"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	topicName       = "max_records_topic"
	maxRecordsCount = 3
	messageCount    = 7

	dataFieldTopicName   = "data_field_topic"
	dataFieldTimeColumn  = "event_time"
	dataFieldRotateEvery = time.Hour

	regularRotationTopic    = "regular_rotation_topic"
	regularRotationInterval = 30 * time.Second
)

// Write times stay well inside the rotator interval, so the only reason a file can close
// is MaxRecordsCount: the next record would push the open file past the limit.
var messageTime = time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)

// A file is uploaded only once the sink rotates away from it, so the last incomplete
// file stays in an open pipe and never lands in the bucket. Completed files are keyed
// by the offset of the message that opened them.
var expectedFiles = map[string][]int{
	objectKey(topicName, 0): {0, 1, 2},
	objectKey(topicName, 3): {3, 4, 5},
}

func objectKey(topic string, startOffset int) string {
	return fmt.Sprintf("%s/partition=0/%s+0+%d.json", topic, topic, startOffset)
}

func TestMaxRecordsCountReplication(t *testing.T) {
	sourceCfg := &kafka.KafkaSource{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafka.KafkaAuth{Enabled: false},
		Topic:            topicName,
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     nil,
	}

	dst := &s3_v1_model.S3Destination{
		Bucket:         envOrDefault("TEST_BUCKET", "barrel"),
		SerializerType: model.ParsingFormatJSON,
		Serializer:     s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},

		BufferSize:     1 * 1024 * 1024,
		BufferInterval: 5 * time.Second,

		Connection: s3_model.ConnectionConfig{
			AccessKey:        envOrDefault("TEST_ACCESS_KEY_ID", "1234567890"),
			S3ForcePathStyle: true,
			SecretKey:        model.SecretString(envOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")),
			Region:           "eu-central1",
		},

		RotatorType: s3_v1_model.DefaultRotator,
		// Far longer than the span of the test data, so every rotation below is caused by
		// MaxRecordsCount rather than by the interval elapsing
		RotatorConfig: s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{
			Interval:        24 * time.Hour,
			MaxRecordsCount: maxRecordsCount,
		}},
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
	}
	dst.WithDefaults()
	require.NoError(t, dst.Validate())
	require.Equal(t, maxRecordsCount, dst.RotatorConfig.Default.MaxRecordsCount)

	if os.Getenv("S3MDS_PORT") != "" {
		dst.Connection.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		s3.CreateBucket(t, dst)
	}

	createTopicAndFillWithData(t, sourceCfg)

	transferhelpers.InitSrcDst(transferhelpers.TransferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer func() { _ = localWorker.Stop() }()

	fileToData := s3.WaitForDestinationData(t, dst, len(expectedFiles))
	time.Sleep(time.Second * 3)

	require.ElementsMatch(t, keys(expectedFiles), keys(fileToData),
		"each file must contain at most MaxRecordsCount records, and only the files the sink has rotated away from may be uploaded")

	for fileName, messageIndexes := range expectedFiles {
		require.LessOrEqual(t, len(messageIndexes), maxRecordsCount, "file %s exceeds MaxRecordsCount", fileName)

		lines := bytes.Split(bytes.TrimSuffix(fileToData[fileName], []byte{'\n'}), []byte{'\n'})
		require.Len(t, lines, len(messageIndexes), "unexpected number of rows in %s", fileName)

		for i, line := range lines {
			var resultMessage map[string]any
			require.NoError(t, json.Unmarshal(line, &resultMessage))
			require.Equal(t, messageValue(messageIndexes[i]), resultMessage["data"])
		}
	}
}

// Kafka write times are all the same, so a rotator that read record metadata would never
// split files. Event times in the payload walk across hour boundaries; the default
// partitioner still names objects by topic/partition/offset, not by those event hours.
var dataFieldEventTimes = []time.Time{
	time.Date(2006, time.January, 2, 15, 0, 0, 0, time.UTC),
	time.Date(2006, time.January, 2, 15, 30, 0, 0, time.UTC),
	time.Date(2006, time.January, 2, 16, 0, 0, 0, time.UTC),
	time.Date(2006, time.January, 2, 16, 30, 0, 0, time.UTC),
	time.Date(2006, time.January, 2, 17, 0, 0, 0, time.UTC),
	time.Date(2006, time.January, 2, 17, 30, 0, 0, time.UTC),
	time.Date(2006, time.January, 2, 18, 0, 0, 0, time.UTC),
}

var expectedDataFieldFiles = map[string][]int{
	objectKey(dataFieldTopicName, 0): {0, 1},
	objectKey(dataFieldTopicName, 2): {2, 3},
	objectKey(dataFieldTopicName, 4): {4, 5},
}

func TestDefaultPartitionerDataFieldTimeExtractor(t *testing.T) {
	parserConfigMap, err := parsers.ParserConfigStructToMap(&parser_json.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: dataFieldTimeColumn, DataType: ytschema.TypeDatetime.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: false,
		Timezone:      "UTC",
	})
	require.NoError(t, err)

	sourceCfg := &kafka.KafkaSource{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafka.KafkaAuth{Enabled: false},
		Topic:            dataFieldTopicName,
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     parserConfigMap,
	}

	dst := &s3_v1_model.S3Destination{
		Bucket:         envOrDefault("TEST_BUCKET", "barrel") + "-data-field",
		SerializerType: model.ParsingFormatJSON,
		Serializer:     s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},

		BufferSize:     1 * 1024 * 1024,
		BufferInterval: 5 * time.Second,

		Connection: s3_model.ConnectionConfig{
			AccessKey:        envOrDefault("TEST_ACCESS_KEY_ID", "1234567890"),
			S3ForcePathStyle: true,
			SecretKey:        model.SecretString(envOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")),
			Region:           "eu-central1",
		},

		RotatorType: s3_v1_model.DefaultRotator,
		RotatorConfig: s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{
			Interval:        dataFieldRotateEvery,
			MaxRecordsCount: 0,
		}},
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
		TimeExtractorType: s3_v1_model.DataFieldTimeExtractor,
		TimeExtractorConfig: s3_v1_model.TimeExtractorUnion{DataField: &s3_v1_model.DataFieldTimeExtractorConfig{
			Column: dataFieldTimeColumn,
		}},
	}
	dst.WithDefaults()
	require.NoError(t, dst.Validate())
	require.Equal(t, s3_v1_model.DataFieldTimeExtractor, dst.TimeExtractorType)
	require.Equal(t, dataFieldTimeColumn, dst.TimeExtractorConfig.DataField.Column)

	if os.Getenv("S3MDS_PORT") != "" {
		dst.Connection.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		s3.CreateBucket(t, dst)
	}

	createTopicAndFillWithParsedData(t, sourceCfg)

	transferID := transferhelpers.GenerateTransferID(t.Name())
	transferhelpers.InitSrcDst(transferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)
	transfer := transferhelpers.MakeTransfer(transferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer func() { _ = localWorker.Stop() }()

	fileToData := s3.WaitForDestinationData(t, dst, len(expectedDataFieldFiles))

	require.ElementsMatch(t, keys(expectedDataFieldFiles), keys(fileToData),
		"rotation must follow event_time from the payload, while object keys stay on the default partitioner layout")

	for fileName := range fileToData {
		require.Contains(t, fileName, dataFieldTopicName+"/partition=0/",
			"default partitioner must keep files under <topic>/partition=<n>/, not a time bucket")
	}

	for fileName, messageIndexes := range expectedDataFieldFiles {
		lines := bytes.Split(bytes.TrimSuffix(fileToData[fileName], []byte{'\n'}), []byte{'\n'})
		require.Len(t, lines, len(messageIndexes), "unexpected number of rows in %s", fileName)

		for i, line := range lines {
			var resultMessage map[string]any
			require.NoError(t, json.Unmarshal(line, &resultMessage))
			require.Equal(t, messageValue(messageIndexes[i]), resultMessage["msg"])

			eventTimeStr, ok := resultMessage[dataFieldTimeColumn].(string)
			require.True(t, ok, "event_time must be serialized as a string in %s", fileName)
			parsedEventTime, err := time.Parse(time.RFC3339, eventTimeStr)
			require.NoError(t, err)
			require.True(t, dataFieldEventTimes[messageIndexes[i]].Equal(parsedEventTime))
		}
	}
}

// Without regular rotation the last open file is never uploaded: nothing arrives to rotate
// away from it. With IsRegularRotationEnabled the wallclock ticker commits that file, so
// every produced record lands in the bucket under the default partitioner layout.
var expectedRegularRotationFiles = map[string][]int{
	objectKey(regularRotationTopic, 0): {0, 1, 2, 3, 4, 5, 6},
}

func TestDefaultPartitionerRegularRotation(t *testing.T) {
	sourceCfg := &kafka.KafkaSource{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafka.KafkaAuth{Enabled: false},
		Topic:            regularRotationTopic,
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     nil,
	}

	dst := &s3_v1_model.S3Destination{
		Bucket:         envOrDefault("TEST_BUCKET", "barrel") + "-regular-rotation",
		SerializerType: model.ParsingFormatJSON,
		Serializer:     s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},

		BufferSize:     1 * 1024 * 1024,
		BufferInterval: time.Second,

		Connection: s3_model.ConnectionConfig{
			AccessKey:        envOrDefault("TEST_ACCESS_KEY_ID", "1234567890"),
			S3ForcePathStyle: true,
			SecretKey:        model.SecretString(envOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")),
			Region:           "eu-central1",
		},

		RotatorType: s3_v1_model.DefaultRotator,
		// Interval is the wallclock period of the regular committer. Message timestamps
		// are identical, so ShouldRotate never fires on event time and the only commit
		// is the ticker.
		RotatorConfig: s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{
			Interval:                 regularRotationInterval,
			MaxRecordsCount:          0,
			IsRegularRotationEnabled: true,
		}},
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
	}
	dst.WithDefaults()
	require.NoError(t, dst.Validate())
	require.True(t, dst.RotatorConfig.Default.IsRegularRotationEnabled)

	if os.Getenv("S3MDS_PORT") != "" {
		dst.Connection.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		s3.CreateBucket(t, dst)
	}

	createTopicAndFillWithData(t, sourceCfg)

	transferID := transferhelpers.GenerateTransferID(t.Name())
	transferhelpers.InitSrcDst(transferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)
	transfer := transferhelpers.MakeTransfer(transferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer func() { _ = localWorker.Stop() }()

	fileToData := s3.WaitForDestinationData(t, dst, len(expectedRegularRotationFiles))

	require.ElementsMatch(t, keys(expectedRegularRotationFiles), keys(fileToData),
		"regular rotation must commit the open file on wallclock, and object keys must follow the default partitioner")

	for fileName := range fileToData {
		require.Contains(t, fileName, regularRotationTopic+"/partition=0/",
			"default partitioner must keep files under <topic>/partition=<n>/, not a time bucket")
	}

	for fileName, messageIndexes := range expectedRegularRotationFiles {
		lines := bytes.Split(bytes.TrimSuffix(fileToData[fileName], []byte{'\n'}), []byte{'\n'})
		require.Len(t, lines, len(messageIndexes), "unexpected number of rows in %s", fileName)

		for i, line := range lines {
			var resultMessage map[string]any
			require.NoError(t, json.Unmarshal(line, &resultMessage))
			require.Equal(t, messageValue(messageIndexes[i]), resultMessage["data"])
		}
	}
}

func messageValue(idx int) string {
	return fmt.Sprintf("test_message offset %d", idx)
}

func keys[V any](m map[string]V) []string {
	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

func envOrDefault(key string, def string) string {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}
	return def
}

// createTopicAndFillWithData writes messageCount messages into a single partition, so that
// offsets and the resulting file names are fully deterministic
func createTopicAndFillWithData(t *testing.T, sourceCfg *kafka.KafkaSource) {
	cl := newClient(t, sourceCfg)
	defer cl.Close()

	ctx := context.Background()
	createResponse, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, sourceCfg.Topic)
	require.NoError(t, err)
	require.NoError(t, createResponse.Err)

	records := make([]*kgo.Record, 0, messageCount)
	for i := 0; i < messageCount; i++ {
		records = append(records, &kgo.Record{
			Value:     []byte(messageValue(i)),
			Timestamp: messageTime,
			Topic:     sourceCfg.Topic,
			Partition: 0,
		})
	}

	require.NoError(t, cl.ProduceSync(ctx, records...).FirstErr())
}

// createTopicAndFillWithParsedData writes JSON records whose event_time disagrees with the
// Kafka write timestamp. All write timestamps are identical, so only a data-field extractor
// can produce the hourly file split expected by TestDefaultPartitionerDataFieldTimeExtractor.
func createTopicAndFillWithParsedData(t *testing.T, sourceCfg *kafka.KafkaSource) {
	cl := newClient(t, sourceCfg)
	defer cl.Close()

	ctx := context.Background()
	createResponse, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, dataFieldTopicName)
	require.NoError(t, err)
	require.NoError(t, createResponse.Err)

	writeTime := time.Date(2006, time.January, 2, 10, 0, 0, 0, time.UTC)
	records := make([]*kgo.Record, 0, len(dataFieldEventTimes))
	for i, eventTime := range dataFieldEventTimes {
		payload, err := json.Marshal(map[string]string{
			dataFieldTimeColumn: eventTime.Format(time.RFC3339),
			"msg":               messageValue(i),
		})
		require.NoError(t, err)
		records = append(records, &kgo.Record{
			Value:     payload,
			Timestamp: writeTime,
			Topic:     dataFieldTopicName,
			Partition: 0,
		})
	}

	require.NoError(t, cl.ProduceSync(ctx, records...).FirstErr())
}

func newClient(t *testing.T, sourceCfg *kafka.KafkaSource) *kgo.Client {
	brokers, err := kafka.ResolveBrokers(sourceCfg.Connection)
	require.NoError(t, err)
	tlsConfig, err := sourceCfg.Connection.TLSConfig()
	require.NoError(t, err)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)

	return cl
}
