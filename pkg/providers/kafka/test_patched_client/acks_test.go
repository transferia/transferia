package test_patched_client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	kafka_writer "github.com/transferia/transferia/pkg/providers/kafka/writer"
	serializer "github.com/transferia/transferia/pkg/serializer/queue"
	"go.ytsaurus.tech/library/go/core/log/nop"
)

func TestWriteMessagesReturnsBrokerRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	brokers := strings.Split(os.Getenv("KAFKA_RECIPE_BROKER_LIST"), ",")
	require.NotEmpty(t, brokers[0], "the Kafka recipe must supply a real broker")
	client, topic := newAckTestTopic(t, ctx, brokers)

	lgr := &nop.Logger{}
	writer := kafka_writer.NewWriter(brokers, kafka.Compression(0), nil, nil, nil, 0, nil)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })

	payload := strings.Repeat("x", 128<<10)
	messages := []serializer.SerializedMessage{{Key: []byte("key"), Value: []byte(payload)}}

	// Healthy Kafka: accept and read back the payload, warming this writer's
	// per-topic size cache while max.message.bytes is 1 MiB.
	require.NoError(t, writer.WriteMessages(ctx, lgr, topic, messages))
	requireAckTestReadback(t, ctx, brokers, topic, messages[0])

	// Rejection precondition: RequireOne must observe the broker rejecting
	// this same payload after its limit drops to 4 KiB.
	setAckTestTopicLimit(t, ctx, client, topic, 4096)
	expectedBrokerError := waitForSizeRejection(t, ctx, client, topic, payload)
	before := lastOffset(t, ctx, client, topic)

	// Reuse the warmed production writer: a fresh one would discover the new
	// limit and reject locally before exercising broker-error propagation.
	writeErr := writer.WriteMessages(ctx, lgr, topic, messages)
	require.Error(t, writeErr,
		"EXPECTED: WriteMessages returns Kafka error MessageSizeTooLarge\nACTUAL: WriteMessages returned OK (nil)\nConfirmed broker response: %v", expectedBrokerError)
	require.ErrorIs(t, writeErr, kafka.MessageSizeTooLarge,
		"WriteMessages must return the confirmed broker rejection, not an unrelated error")
	require.Equal(t, before, lastOffset(t, ctx, client, topic), "the rejected message must not be appended")
}

func newAckTestTopic(t *testing.T, ctx context.Context, brokers []string) (*kafka.Client, string) {
	t.Helper()
	transport := &kafka.Transport{DialTimeout: 3 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 3 * time.Second, Transport: transport}
	topic := fmt.Sprintf("acks-rejection-%d", time.Now().UnixNano())
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{{ConfigName: "max.message.bytes", ConfigValue: strconv.Itoa(1 << 20)}},
		}},
	})
	require.NoError(t, err)
	require.Contains(t, created.Errors, topic)
	require.NoError(t, created.Errors[topic])
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		deleted, err := client.DeleteTopics(cleanupCtx, &kafka.DeleteTopicsRequest{Topics: []string{topic}})
		require.NoError(t, err)
		require.Contains(t, deleted.Errors, topic)
		require.NoError(t, deleted.Errors[topic])
	})
	return client, topic
}

func requireAckTestReadback(t *testing.T, ctx context.Context, brokers []string, topic string, expected serializer.SerializedMessage) {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, Partition: 0,
		MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 100 * time.Millisecond,
	})
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	written, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, expected.Key, written.Key)
	require.Equal(t, expected.Value, written.Value)
	require.Equal(t, int64(0), written.Offset)
}

func setAckTestTopicLimit(t *testing.T, ctx context.Context, client *kafka.Client, topic string, limit int) {
	t.Helper()
	changed, err := client.AlterConfigs(ctx, &kafka.AlterConfigsRequest{
		Resources: []kafka.AlterConfigRequestResource{{
			ResourceType: kafka.ResourceTypeTopic, ResourceName: topic,
			Configs: []kafka.AlterConfigRequestConfig{{Name: "max.message.bytes", Value: strconv.Itoa(limit)}},
		}},
	})
	require.NoError(t, err)
	resource := kafka.AlterConfigsResponseResource{Type: int8(kafka.ResourceTypeTopic), Name: topic}
	require.Contains(t, changed.Errors, resource)
	require.NoError(t, changed.Errors[resource])
}

func waitForSizeRejection(t *testing.T, ctx context.Context, client *kafka.Client, topic, payload string) error {
	t.Helper()
	for {
		response, err := client.Produce(ctx, &kafka.ProduceRequest{
			Topic: topic, Partition: 0, RequiredAcks: kafka.RequireOne,
			Records: kafka.NewRecordReader(kafka.Record{Key: kafka.NewBytes([]byte("control")), Value: kafka.NewBytes([]byte(payload))}),
		})
		require.NoError(t, err)
		require.NotNil(t, response)
		if errors.Is(response.Error, kafka.MessageSizeTooLarge) {
			return response.Error
		}
		require.NoError(t, response.Error, "unexpected broker error while waiting for the size limit")
		select {
		case <-ctx.Done():
			t.Fatalf("broker did not enforce max.message.bytes: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func lastOffset(t *testing.T, ctx context.Context, client *kafka.Client, topic string) int64 {
	t.Helper()
	response, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{topic: {kafka.LastOffsetOf(0)}},
	})
	require.NoError(t, err)
	require.Len(t, response.Topics[topic], 1)
	partition := response.Topics[topic][0]
	require.NoError(t, partition.Error)
	return partition.LastOffset
}
