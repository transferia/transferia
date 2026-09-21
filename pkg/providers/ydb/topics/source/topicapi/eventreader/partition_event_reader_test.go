package eventreader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb/recipe"
	ydbtopic "github.com/transferia/transferia/tests/helpers/ydb/recipe/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func TestCommitOffsets(t *testing.T) {
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicName := "test_topic_commit_offsets"
	topicPartition := int64(0)
	testData := [][]byte{[]byte("message_1"), []byte("message_2"), []byte("message_3")}
	ydbtopic.CreateAndFillTopicWithDriver(t, topicName, testData, ydbClient)

	// check offset before commit
	require.Equal(t, int64(0), fetchCommittedOffset(t, topicName, ydbtopic.DefaultConsumer, ydbClient))

	// commit
	eventReader, err := NewPartitionEventReader(ydbtopic.DefaultConsumer, topicName, topicPartition, ydbClient, logger.Log)
	require.NoError(t, err)

	require.NoError(t, eventReader.CommitOffset(context.Background(), 1))

	// check offset after commit
	require.Equal(t, int64(2), fetchCommittedOffset(t, topicName, ydbtopic.DefaultConsumer, ydbClient))

	require.NoError(t, eventReader.Close(context.Background()))
}

func fetchCommittedOffset(t *testing.T, topicName, consumer string, ydbClient *ydb.Driver) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	description, err := ydbClient.Topic().DescribeTopicConsumer(ctx, topicName, consumer, topicoptions.IncludeConsumerStats())
	require.NoError(t, err)

	require.Len(t, description.Partitions, 1)

	return description.Partitions[0].PartitionConsumerStats.CommittedOffset
}
