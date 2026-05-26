package topicapisource

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
)

func TestPartitionListerListPartitions(t *testing.T) {
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	t.Run("OneTopicPartition", func(t *testing.T) {
		topicName := "test_list_partitions_topic"
		_ = createTopicAndFillWithData(t, ydbClient, topicName)

		sourceCfg := newTestSourceCfg(t, []string{topicName})

		lister, err := NewPartitionLister(sourceCfg, logger.Log)
		require.NoError(t, err)
		defer func() {
			lister.Close()
		}()

		partitions, err := lister.ListPartitions()
		require.NoError(t, err)
		require.Len(t, partitions, 3)

		expectedPartitions := []abstract.Partition{
			{Topic: topicName, Partition: 0},
			{Topic: topicName, Partition: 1},
			{Topic: topicName, Partition: 2},
		}
		slices.SortFunc(partitions, func(a, b abstract.Partition) int {
			return int(a.Partition) - int(b.Partition)
		})

		require.Equal(t, expectedPartitions, partitions)
	})

	t.Run("FewTopics", func(t *testing.T) {
		topicName1 := "test_list_partitions_few_topics_1"
		_ = createTopicAndFillWithData(t, ydbClient, topicName1)
		topicName2 := "test_list_partitions_few_topics_2"
		_ = createTopicAndFillWithData(t, ydbClient, topicName2)
		topicName3 := "test_list_partitions_few_topics_3"
		_ = createTopicAndFillWithData(t, ydbClient, topicName3)

		sourceCfg := newTestSourceCfg(t, []string{topicName1, topicName2, topicName3})

		lister, err := NewPartitionLister(sourceCfg, logger.Log)
		require.NoError(t, err)
		defer func() {
			lister.Close()
		}()

		partitions, err := lister.ListPartitions()
		require.NoError(t, err)
		require.Len(t, partitions, 9)

		expectedPartitions := []abstract.Partition{
			{Topic: topicName1, Partition: 0},
			{Topic: topicName1, Partition: 1},
			{Topic: topicName1, Partition: 2},
			{Topic: topicName2, Partition: 0},
			{Topic: topicName2, Partition: 1},
			{Topic: topicName2, Partition: 2},
			{Topic: topicName3, Partition: 0},
			{Topic: topicName3, Partition: 1},
			{Topic: topicName3, Partition: 2},
		}
		slices.SortFunc(partitions, func(a, b abstract.Partition) int {
			if a.Topic == b.Topic {
				return int(a.Partition) - int(b.Partition)
			}
			if a.Topic <= b.Topic {
				return -1
			}
			return 1
		})

		require.Equal(t, expectedPartitions, partitions)
	})
}
