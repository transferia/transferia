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

const (
	topicName = "time_based_topic"
	// Asia/Tokyo is UTC+9 and never observes DST, so a local day boundary is a plain 15:00 UTC cut
	timezone = "Asia/Tokyo"
)

// Write times are picked so the UTC date and the Asia/Tokyo date disagree: every message is
// written on January 2nd or 3rd UTC, yet they belong to three different Tokyo days. A
// partitioner that ignored Timezone would put the first four into a single 2006/01/02
// directory, so the expected layout below only holds if the timezone is really applied.
var testMessages = []struct {
	writeTime time.Time
	dir       string // Tokyo day the message belongs to
}{
	{time.Date(2006, time.January, 2, 10, 0, 0, 0, time.UTC), "2006/01/02"}, // Tokyo Jan 2, 19:00
	{time.Date(2006, time.January, 2, 12, 0, 0, 0, time.UTC), "2006/01/02"}, // Tokyo Jan 2, 21:00
	{time.Date(2006, time.January, 2, 15, 0, 0, 0, time.UTC), "2006/01/03"}, // Tokyo Jan 3, 00:00 - local midnight, still Jan 2 in UTC
	{time.Date(2006, time.January, 2, 20, 0, 0, 0, time.UTC), "2006/01/03"}, // Tokyo Jan 3, 05:00
	{time.Date(2006, time.January, 3, 15, 0, 0, 0, time.UTC), "2006/01/04"}, // Tokyo Jan 4, 00:00 - local midnight
	{time.Date(2006, time.January, 3, 18, 0, 0, 0, time.UTC), "2006/01/04"}, // Tokyo Jan 4, 03:00
}

// A file is uploaded only once the sink rotates away from it, so the last Tokyo day stays in
// an open pipe and never lands in the bucket. The two completed files are keyed by the offset
// of the message that opened them.
var expectedFiles = map[string][]int{
	objectKey("2006/01/02", 0): {0, 1},
	objectKey("2006/01/03", 2): {2, 3},
}

func objectKey(dir string, startOffset int) string {
	return fmt.Sprintf("%s/%s/%s+0+%d.json", topicName, dir, topicName, startOffset)
}

func TestTimeBasedPartitionerReplication(t *testing.T) {
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
		// Far longer than the span of the test data, so every rotation below is caused by the
		// partitioner directory changing rather than by the interval elapsing
		RotatorConfig:   s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: 24 * time.Hour}},
		PartitionerType: s3_v1_model.TimeBasedPartitioner,
		PartitionerConfig: s3_v1_model.PartitionerUnion{TimeBased: &s3_v1_model.TimeBasedPartitionerConfig{
			PartitionType: s3_v1_model.TimePartitionDay,
			Timezone:      timezone,
		}},
	}
	dst.WithDefaults()
	require.NoError(t, dst.Validate())

	if os.Getenv("S3MDS_PORT") != "" {
		dst.Connection.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		s3.CreateBucket(t, dst)
	}

	createTopicAndFillWithData(t, sourceCfg)

	helpers.InitSrcDst(helpers.TransferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, sourceCfg, dst, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer func() { _ = localWorker.Stop() }()

	fileToData := s3.WaitForDestinationData(t, dst, len(expectedFiles))

	require.ElementsMatch(t, keys(expectedFiles), keys(fileToData),
		"each Asia/Tokyo day must get its own directory, and only the days the sink has rotated away from may be uploaded")

	for fileName, messageIndexes := range expectedFiles {
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

// createTopicAndFillWithData writes testMessages into a single partition, so that offsets and
// the resulting file names are fully deterministic
func createTopicAndFillWithData(t *testing.T, sourceCfg *kafka.KafkaSource) {
	cl := newClient(t, sourceCfg)
	defer cl.Close()

	ctx := context.Background()
	createResponse, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, topicName)
	require.NoError(t, err)
	require.NoError(t, createResponse.Err)

	records := make([]*kgo.Record, 0, len(testMessages))
	for i, message := range testMessages {
		records = append(records, &kgo.Record{
			Value:     []byte(messageValue(i)),
			Timestamp: message.writeTime,
			Topic:     topicName,
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
