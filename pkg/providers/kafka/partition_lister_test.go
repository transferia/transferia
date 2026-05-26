package kafka

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestPartitionListerListPartitions(t *testing.T) {
	t.Run("OneTopicPartition", func(t *testing.T) {
		topicName := "test_list_partitions_topic"

		kafkaCfg, err := SourceRecipe()
		require.NoError(t, err)

		_ = createTopicAndFillWithData(t, topicName, kafkaCfg)

		kafkaCfg.Topic = topicName

		lister, err := NewPartitionLister(kafkaCfg)
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
		kafkaCfg, err := SourceRecipe()
		require.NoError(t, err)

		topicName1 := "test_list_partitions_few_topics_1"
		_ = createTopicAndFillWithData(t, topicName1, kafkaCfg)
		topicName2 := "test_list_partitions_few_topics_2"
		_ = createTopicAndFillWithData(t, topicName2, kafkaCfg)
		topicName3 := "test_list_partitions_few_topics_3"
		_ = createTopicAndFillWithData(t, topicName3, kafkaCfg)

		kafkaCfg.GroupTopics = []string{topicName1, topicName2, topicName3}
		kafkaCfg.Topic = ""

		lister, err := NewPartitionLister(kafkaCfg)
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
