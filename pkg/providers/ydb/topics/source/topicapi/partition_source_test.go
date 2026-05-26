package topicapisource

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader"
	"github.com/transferia/transferia/pkg/stats"
	sourcehelpers "github.com/transferia/transferia/tests/helpers/source"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	ydbtopic "github.com/transferia/transferia/tests/helpers/ydb_recipe/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func TestPartitionSource(t *testing.T) {
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	sourceMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))

	t.Run("FullyReadOnePartition", func(t *testing.T) {
		topicName := "fully_read_topic"
		topicPartition := int64(2)
		testData := createTopicAndFillWithData(t, ydbClient, topicName)

		sourceCfg := newTestSourceCfg(t, []string{topicName})
		partitionDesc := PartitionDescription{
			Topic:     topicName,
			Partition: topicPartition,
		}

		src, err := NewPartitionSource(sourceCfg, partitionDesc, nil, logger.Log, sourceMetrics)
		require.NoError(t, err)
		result, err := sourcehelpers.WaitForItemsQueueToS3(src, 10, 500*time.Millisecond)
		require.NoError(t, err)

		topicPartitionData, ok := testData[topicPartition]
		require.True(t, ok)

		concatenatedResult := slices.Concat(result...)
		for idx, item := range concatenatedResult {
			require.Equal(t, topicName, item.QueueMessageMeta.TopicName)
			require.Equal(t, topicPartition, int64(item.QueueMessageMeta.PartitionNum))
			require.Equal(t, string(topicPartitionData[idx]), item.ColumnValues[4])
		}

		committedOffsets := fetchOffsets(t, ydbClient, topicName, sourceCfg.Consumer)
		for partition, offset := range committedOffsets {
			if partition == topicPartition {
				require.Equal(t, int64(10), offset)
			} else {
				require.Equal(t, int64(0), offset)
			}
		}
	})

	t.Run("ReadOnePartitionFromSomeOffset", func(t *testing.T) {
		topicName := "read_topic_from_some_offset"
		topicPartition := int64(2)
		testData := createTopicAndFillWithData(t, ydbClient, topicName)

		sourceCfg := newTestSourceCfg(t, []string{topicName})
		partitionDesc := PartitionDescription{
			Topic:     topicName,
			Partition: topicPartition,
		}

		// create tmp source and commit a few messages before run
		src, err := NewPartitionSource(sourceCfg, partitionDesc, nil, logger.Log, sourceMetrics)
		require.NoError(t, err)

		committableReader, ok := src.reader.(eventreader.OffsetCommitEventReader)
		require.True(t, ok)
		require.NoError(t, committableReader.CommitOffset(context.Background(), 2))
		src.Stop()
		committedOffsets := fetchOffsets(t, ydbClient, topicName, sourceCfg.Consumer)
		require.Equal(t, int64(3), committedOffsets[topicPartition]) // in ydb topics, a committed offset is the first message to be read

		// run source
		src, err = NewPartitionSource(sourceCfg, partitionDesc, nil, logger.Log, sourceMetrics)
		require.NoError(t, err)
		result, err := sourcehelpers.WaitForItemsQueueToS3(src, 7, 500*time.Millisecond)
		require.NoError(t, err)

		topicPartitionData, ok := testData[topicPartition]
		require.True(t, ok)
		topicPartitionData = topicPartitionData[3:]

		concatenatedResult := slices.Concat(result...)
		for idx, item := range concatenatedResult {
			require.Equal(t, topicName, item.QueueMessageMeta.TopicName)
			require.Equal(t, topicPartition, int64(item.QueueMessageMeta.PartitionNum))
			require.Equal(t, string(topicPartitionData[idx]), item.ColumnValues[4])
		}

		committedOffsets = fetchOffsets(t, ydbClient, topicName, sourceCfg.Consumer)
		for partition, offset := range committedOffsets {
			if partition == topicPartition {
				require.Equal(t, int64(10), offset)
			} else {
				require.Equal(t, int64(0), offset)
			}
		}
	})

	t.Run("ReadOnePartitionWithTwoSourceRuns", func(t *testing.T) {
		topicName := "read_topic_with_two_source_runs"
		topicPartition := int64(2)
		testData := createTopicAndFillWithData(t, ydbClient, topicName)

		sourceCfg := newTestSourceCfg(t, []string{topicName})
		partitionDesc := PartitionDescription{
			Topic:     topicName,
			Partition: topicPartition,
		}

		// first run
		src, err := NewPartitionSource(sourceCfg, partitionDesc, nil, logger.Log, sourceMetrics)
		require.NoError(t, err)
		result, err := sourcehelpers.WaitForItemsQueueToS3(src, 10, 500*time.Millisecond)
		require.NoError(t, err)

		topicPartitionData, ok := testData[topicPartition]
		require.True(t, ok)

		concatenatedResult := slices.Concat(result...)
		for idx, item := range concatenatedResult {
			require.Equal(t, topicName, item.QueueMessageMeta.TopicName)
			require.Equal(t, topicPartition, int64(item.QueueMessageMeta.PartitionNum))
			require.Equal(t, string(topicPartitionData[idx]), item.ColumnValues[4])
		}

		// write new messages
		partitionTestData := make([][]byte, 5)
		for i := range partitionTestData {
			partitionTestData[i] = []byte(fmt.Sprintf("new test_message offset %d, partition 2", i))
		}
		ydbtopic.WriteMessages(t, topicName, partitionTestData, ydbClient, topicoptions.WithWriterPartitionID(topicPartition))

		// second run
		src, err = NewPartitionSource(sourceCfg, partitionDesc, nil, logger.Log, sourceMetrics)
		require.NoError(t, err)
		result, err = sourcehelpers.WaitForItemsQueueToS3(src, 5, 500*time.Millisecond)
		require.NoError(t, err)

		concatenatedResult = slices.Concat(result...)
		for idx, item := range concatenatedResult {
			require.Equal(t, topicName, item.QueueMessageMeta.TopicName)
			require.Equal(t, topicPartition, int64(item.QueueMessageMeta.PartitionNum))
			require.Equal(t, string(partitionTestData[idx]), item.ColumnValues[4])
		}

		committedOffsets := fetchOffsets(t, ydbClient, topicName, sourceCfg.Consumer)
		for partition, offset := range committedOffsets {
			if partition == topicPartition {
				require.Equal(t, int64(15), offset)
			} else {
				require.Equal(t, int64(0), offset)
			}
		}
	})
}

func TestPartitionSourceFetch(t *testing.T) {
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicName := "topic_partition_fetch_test"
	topicPartition := int64(2)
	testData := createTopicAndFillWithData(t, ydbClient, topicName)

	sourceCfg := newTestSourceCfg(t, []string{topicName})
	partitionDesc := PartitionDescription{
		Topic:     topicName,
		Partition: topicPartition,
	}

	sourceMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	src, err := NewPartitionSource(sourceCfg, partitionDesc, nil, logger.Log, sourceMetrics)
	require.NoError(t, err)
	defer src.Stop()

	res, err := src.Fetch()
	require.NoError(t, err)

	topicPartitionData, ok := testData[topicPartition]
	require.True(t, ok)

	for idx, item := range res {
		require.Equal(t, topicName, item.Table)
		require.Equal(t, topicName, item.QueueMessageMeta.TopicName)
		require.Equal(t, string(topicPartitionData[idx]), item.ColumnValues[4])
	}
}

func TestPartitionSourceCheckTopic(t *testing.T) {
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicName := "absent_topic"

	sourceCfg := newTestSourceCfg(t, []string{topicName})
	partitionDesc := PartitionDescription{
		Topic:     topicName,
		Partition: 2,
	}

	sourceMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	_, err := NewPartitionSource(sourceCfg, partitionDesc, nil, logger.Log, sourceMetrics)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))

	// check error code
	var unwrapErr interface {
		Unwrap() error
	}
	require.ErrorAs(t, err, &unwrapErr)

	var codedErr interface {
		Code() coded.Code
	}
	require.ErrorAs(t, unwrapErr.Unwrap(), &codedErr)
	require.Equal(t, codes.MissingData, codedErr.Code())
}

func createTopicAndFillWithData(t *testing.T, ydbClient *ydb.Driver, topicName string) map[int64][][]byte {
	ydbtopic.CreateTopic(t, topicName, ydbClient, topicoptions.CreateWithMinActivePartitions(3))

	testData := make(map[int64][][]byte)
	for partition := int64(0); partition < 3; partition++ {
		for i := 0; i < 10; i++ {
			val := []byte(fmt.Sprintf("test_message offset %d, partition %d", i, partition))
			testData[partition] = append(testData[partition], val)
		}
	}

	for partition, messages := range testData {
		ydbtopic.WriteMessages(t, topicName, messages, ydbClient, topicoptions.WithWriterPartitionID(partition))
	}

	return testData
}

func fetchOffsets(t *testing.T, ydbClient *ydb.Driver, topicName, consumer string) map[int64]int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	description, err := ydbClient.Topic().DescribeTopicConsumer(ctx, topicName, consumer, topicoptions.IncludeConsumerStats())
	require.NoError(t, err)

	committedOffsets := make(map[int64]int64)
	for _, partition := range description.Partitions {
		committedOffsets[partition.PartitionID] = partition.PartitionConsumerStats.CommittedOffset
	}

	return committedOffsets
}
