package kafka

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	sourcehelpers "github.com/transferia/transferia/tests/helpers/source"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPartitionSource(t *testing.T) {
	kafkaCfgTemplate, err := SourceRecipe()
	require.NoError(t, err)

	t.Run("FullyReadOnePartition", func(t *testing.T) {
		topicName := "fully_read_topic"
		topicPartition := int32(2)

		kafkaCfg := *kafkaCfgTemplate
		kafkaCfg.Topic = topicName
		partitionDesc := PartitionDescription{
			Topic:     topicName,
			Partition: topicPartition,
		}

		testData := createTopicAndFillWithData(t, topicName, &kafkaCfg)

		src, err := NewPartitionSource("dtt", &kafkaCfg, partitionDesc, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		result, err := sourcehelpers.WaitForItemsQueueToS3(src, 10, 500*time.Millisecond)
		require.NoError(t, err)

		topicPartitionData, ok := testData[topicPartition]
		require.True(t, ok)

		concatenatedResult := slices.Concat(result...)
		for idx, item := range concatenatedResult {
			require.Equal(t, topicName, item.QueueMessageMeta.TopicName)
			require.Equal(t, topicPartition, int32(item.QueueMessageMeta.PartitionNum))
			require.Equal(t, string(topicPartitionData[idx]), item.ColumnValues[4])
		}

		committedOffsets := fetchOffsets(t, "dtt", &kafkaCfg)
		require.NotNil(t, committedOffsets)
		require.Equal(t, int64(9), committedOffsets[topicName][partitionDesc.Partition].At)
	})

	t.Run("ReadOnePartitionFromSomeOffset", func(t *testing.T) {
		topicName := "read_topic_from_some_offset"
		topicPartition := int32(2)

		kafkaCfg := *kafkaCfgTemplate
		kafkaCfg.Topic = topicName
		partitionDesc := PartitionDescription{
			Topic:     topicName,
			Partition: topicPartition,
		}

		testData := createTopicAndFillWithData(t, topicName, &kafkaCfg)

		// create tmp source and commit a few messages before run
		src, err := NewPartitionSource("dtt", &kafkaCfg, partitionDesc, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		require.NoError(t, src.reader.CommitMessages(context.Background(), []kgo.Record{
			{Topic: topicName, Partition: topicPartition, Offset: 0},
			{Topic: topicName, Partition: topicPartition, Offset: 1},
			{Topic: topicName, Partition: topicPartition, Offset: 2},
		}...))
		src.Stop()

		src, err = NewPartitionSource("dtt", &kafkaCfg, partitionDesc, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		result, err := sourcehelpers.WaitForItemsQueueToS3(src, 7, 500*time.Millisecond)
		require.NoError(t, err)

		topicPartitionData, ok := testData[topicPartition]
		require.True(t, ok)

		topicPartitionData = topicPartitionData[3:]

		concatenatedResult := slices.Concat(result...)
		for idx, item := range concatenatedResult {
			require.Equal(t, topicName, item.QueueMessageMeta.TopicName)
			require.Equal(t, topicPartition, int32(item.QueueMessageMeta.PartitionNum))
			require.Equal(t, string(topicPartitionData[idx]), item.ColumnValues[4])
		}

		committedOffsets := fetchOffsets(t, "dtt", &kafkaCfg)
		require.NotNil(t, committedOffsets)
		require.Equal(t, int64(9), committedOffsets[topicName][partitionDesc.Partition].At)
	})
}

func createTopicAndFillWithData(t *testing.T, topicName string, sourceCfg *KafkaSource) map[int32][][]byte {
	cl := newClient(t, sourceCfg)
	defer cl.Close()

	admCl := kadm.NewClient(cl)

	ctx := context.Background()
	createResponse, err := admCl.CreateTopic(ctx, 3, 1, nil, topicName)
	require.NoError(t, err)
	require.NoError(t, createResponse.Err)

	testData := make(map[int32][][]byte)
	records := make([]*kgo.Record, 0)
	for partition := int32(0); partition < 3; partition++ {
		for i := 0; i < 10; i++ {
			val := []byte(fmt.Sprintf("test_message offset %d, partition %d", i, partition))
			records = append(records, &kgo.Record{
				Topic:     topicName,
				Partition: partition,
				Value:     val,
			})

			testData[partition] = append(testData[partition], val)
		}
	}

	produceRes := cl.ProduceSync(ctx, records...)
	require.NoError(t, produceRes.FirstErr())

	return testData
}

func fetchOffsets(t *testing.T, group string, sourceCfg *KafkaSource) map[string]map[int32]kadm.Offset {
	cl := newClient(t, sourceCfg)
	defer cl.Close()

	admCl := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	resTmp, err := admCl.ListGroups(ctx)
	require.NoError(t, err)
	groups := resTmp.Groups()
	require.NotNil(t, groups)

	res, err := admCl.FetchOffsets(ctx, group)
	require.NoError(t, err)
	require.NoError(t, res.Error())
	off := res.Offsets()

	return off
}

func newClient(t *testing.T, sourceCfg *KafkaSource) *kgo.Client {
	brokers, err := ResolveBrokers(sourceCfg.Connection)
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
