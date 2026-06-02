package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/kafka"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	_ "github.com/transferia/transferia/pkg/providers/s3/provider"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/s3"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap/zapcore"
)

func TestReplication(t *testing.T) {
	topicName := "some_topic"
	var partitionCount int32 = 6

	// Prepare endpoint configs
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

		RotatorType:       s3_v1_model.DefaultRotator,
		RotatorConfig:     s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: time.Hour}},
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
	}
	dst.WithDefaults()

	if os.Getenv("S3MDS_PORT") != "" {
		dst.Connection.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		s3.CreateBucket(t, dst)
	}

	// Prepare test topic
	testDataLen := 0
	testData := createTopicAndFillWithData(t, topicName, partitionCount, sourceCfg)
	for _, messages := range testData {
		testDataLen += len(messages)
	}

	// activate transfer
	helpers.InitSrcDst(helpers.TransferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer func() { _ = localWorker.Stop() }()

	// Check result
	fileToData := s3.WaitForDestinationData(t, dst, int(partitionCount))

	require.Len(t, fileToData, int(partitionCount), "Expected one file per partition")

	partitionToData := make(map[int][]byte)
	for fileName, data := range fileToData {
		// Verify path format: some_topic/partition=N/some_topic+N+0.json
		require.True(t, strings.HasPrefix(fileName, topicName+"/partition="), "Key should start with <topic_name>/partition=")
		require.True(t, strings.HasSuffix(fileName, ".json"), "Key should end with .json")

		// Extract partition number from path
		var partition int
		_, err := fmt.Sscanf(fileName, topicName+"/partition=%d/", &partition)
		require.NoError(t, err)

		partitionToData[partition] = data
	}

	for partition, expectedMessages := range testData {
		resultData, ok := partitionToData[partition]
		require.True(t, ok)

		lines := bytes.Split(bytes.TrimSuffix(resultData, []byte{'\n'}), []byte{'\n'})
		require.Len(t, lines, len(expectedMessages)-1)

		for i, line := range lines {
			var resultMessage map[string]any
			require.NoError(t, json.Unmarshal(line, &resultMessage))

			require.Contains(t, resultMessage, "data")
			require.Equal(t, string(expectedMessages[i]), resultMessage["data"])
		}
	}
}

func envOrDefault(key string, def string) string {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}
	return def
}

func createTopicAndFillWithData(t *testing.T, topicName string, partitionCount int32, sourceCfg *kafka.KafkaSource) map[int][][]byte {
	cl := newClient(t, sourceCfg)
	defer cl.Close()

	admCl := kadm.NewClient(cl)

	ctx := context.Background()
	createResponse, err := admCl.CreateTopic(ctx, partitionCount, 1, nil, topicName)
	require.NoError(t, err)
	require.NoError(t, createResponse.Err)

	testData := make(map[int][][]byte)
	records := make([]*kgo.Record, 0)
	for partition := int32(0); partition < partitionCount; partition++ {
		createTime, _ := time.Parse("2006-01-02 15:04:05", "2006-01-02 15:00:00")
		for i := 0; i < 7; i++ {
			val := []byte(fmt.Sprintf("test_message offset %d, partition %d", i, partition))
			records = append(records, &kgo.Record{
				Value:     val,
				Timestamp: createTime,
				Topic:     topicName,
				Partition: partition,
			})
			createTime = createTime.Add(10 * time.Minute)

			testData[int(partition)] = append(testData[int(partition)], val)
		}
	}

	produceRes := cl.ProduceSync(ctx, records...)
	require.NoError(t, produceRes.FirstErr())

	return testData
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
