package ydb

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	ydbtopic "github.com/transferia/transferia/tests/helpers/ydb_recipe/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestMakeTablePath(t *testing.T) {
	require.Equal(t, "a/b", makeTablePathFromTopicPath("/local/a/b/dtt", "dtt", "local"))
	require.Equal(t, "cashbacks", makeTablePathFromTopicPath("/ru-central1/b1gnusj8glj8pkr3ru0e/etn01jlrd2bfp06votrk/cashbacks/dtt", "dtt", "/ru-central1/b1gnusj8glj8pkr3ru0e/etn01jlrd2bfp06votrk"))
}

func TestCommittedEndOffsetsForCustomFeed(t *testing.T) {
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	consumer := ydbtopic.DefaultConsumer
	table := "custom_test_table"
	topicName := "test_committed_end_offsets_for_custom_feed"
	topicPath := path.Join(table, topicName)
	ydbtopic.CreateTopic(t, topicPath, ydbClient, topicoptions.CreateWithMinActivePartitions(3))

	for p := range 3 {
		messages := make([]topicwriter.Message, 5)
		for i := range messages {
			messages[i] = topicwriter.Message{
				Data: bytes.NewReader([]byte{byte(i)}),
			}
		}
		ydbtopic.WriteTopicMessages(t, topicPath, messages, ydbClient, topicoptions.WithWriterPartitionID(int64(p)))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance, port, db, _ := ydbrecipe.InstancePortDatabaseCreds(t)

	cfg := &YdbSource{
		Instance:                     fmt.Sprintf("%s:%d", instance, port),
		Database:                     db,
		Token:                        model.SecretString(""),
		Tables:                       []string{table},
		ChangeFeedCustomName:         "",
		ChangeFeedCustomConsumerName: "",
	}

	t.Run("NoCustomFeed", func(t *testing.T) {
		require.NoError(t, CommittedEndOffsetsForCustomFeed(ctx, cfg, ydbClient))

		description, err := ydbClient.Topic().DescribeTopicConsumer(ctx, topicPath, consumer)
		require.NoError(t, err)

		for _, partition := range description.Partitions {
			require.Equal(t, int64(0), partition.PartitionConsumerStats.CommittedOffset)
		}
	})

	t.Run("CustomAbsentFeed", func(t *testing.T) {
		cfg.ChangeFeedCustomName = "unknown_topic"
		cfg.ChangeFeedCustomConsumerName = consumer

		require.Error(t, CommittedEndOffsetsForCustomFeed(ctx, cfg, ydbClient))

		description, err := ydbClient.Topic().DescribeTopicConsumer(ctx, topicPath, consumer)
		require.NoError(t, err)

		for _, partition := range description.Partitions {
			require.Equal(t, int64(0), partition.PartitionConsumerStats.CommittedOffset)
		}
	})

	t.Run("CustomFeed", func(t *testing.T) {
		cfg.ChangeFeedCustomName = topicName
		cfg.ChangeFeedCustomConsumerName = consumer

		require.NoError(t, CommittedEndOffsetsForCustomFeed(ctx, cfg, ydbClient))

		description, err := ydbClient.Topic().DescribeTopicConsumer(ctx, topicPath, consumer)
		require.NoError(t, err)

		for _, partition := range description.Partitions {
			require.Equal(t, partition.PartitionStats.PartitionsOffset.End, partition.PartitionConsumerStats.CommittedOffset)
		}
	})
}

func TestCommitEndOffsetsForCustomFeedWithManyTables(t *testing.T) {
	ydbClient := ydbrecipe.Driver(t)

	consumer := dataTransferConsumerName
	customFeedName := "custom_feed"
	tables := []string{"many_test_table1", "many_test_table2", "many_test_table3"}
	for _, table := range tables {
		prepareTableAndFeed(t, customFeedName, table, 0, 0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance, port, db, _ := ydbrecipe.InstancePortDatabaseCreds(t)

	cfg := &YdbSource{
		Instance:                     fmt.Sprintf("%s:%d", instance, port),
		Database:                     db,
		Token:                        model.SecretString(""),
		Tables:                       tables,
		ChangeFeedCustomName:         customFeedName,
		ChangeFeedCustomConsumerName: consumer,
	}

	require.NoError(t, CommittedEndOffsetsForCustomFeed(ctx, cfg, ydbClient))

	for _, table := range tables {
		description, err := ydbClient.Topic().DescribeTopicConsumer(ctx, path.Join(table, customFeedName), consumer)
		require.NoError(t, err)

		for _, partition := range description.Partitions {
			require.Equal(t, partition.PartitionStats.PartitionsOffset.End, partition.PartitionConsumerStats.CommittedOffset)
		}
	}
}
