package topicapisource

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/parsers"
)

func TestSplitBatch(t *testing.T) {
	topic := "test-topic"
	partition := uint32(0)

	baseBatch := parsers.MessageBatch{
		Topic:     topic,
		Partition: partition,
	}

	t.Run("EmptyInput", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = nil
		batches := splitBatch(batch, 0, 0)

		require.Len(t, batches, 1, "should return single batch for empty input")
		require.Len(t, batches[0].Messages, 0, "batch should be empty")
		require.Equal(t, topic, batches[0].Topic)
		require.Equal(t, partition, batches[0].Partition)
	})

	t.Run("NoLimits", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(5, 100)
		batches := splitBatch(batch, 0, 0)

		require.Len(t, batches, 1, "should return single batch with no limits")
		require.Len(t, batches[0].Messages, 5, "batch should contain all messages")
		require.Equal(t, topic, batches[0].Topic)
		require.Equal(t, partition, batches[0].Partition)
	})

	t.Run("CountLimitOnly", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(10, 100)
		batches := splitBatch(batch, 0, 5)

		require.Len(t, batches, 2, "should split into 2 batches")
		require.Len(t, batches[0].Messages, 5, "first batch should have 5 messages")
		require.Len(t, batches[1].Messages, 5, "second batch should have 5 messages")

		for _, b := range batches {
			require.Equal(t, topic, b.Topic)
			require.Equal(t, partition, b.Partition)
		}
	})

	t.Run("SizeLimitOnly", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(5, 100) // 5 messages * 100 bytes = 500 bytes total
		batches := splitBatch(batch, 300, 0)

		require.Len(t, batches, 2, "should split into 2 batches")
		// First batch: 3 messages (300 bytes)
		require.Len(t, batches[0].Messages, 3, "first batch should have 3 messages")
		// Second batch: 2 messages (200 bytes)
		require.Len(t, batches[1].Messages, 2, "second batch should have 2 messages")

		for _, b := range batches {
			require.Equal(t, topic, b.Topic)
			require.Equal(t, partition, b.Partition)
		}
	})

	t.Run("BothLimitsCountTriggersFirst", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(10, 100)
		batches := splitBatch(batch, 300, 3)

		// Count limit (3) triggers before size limit (300)
		require.Len(t, batches, 4, "should split into 4 batches")
		require.Len(t, batches[0].Messages, 3)
		require.Len(t, batches[1].Messages, 3)
		require.Len(t, batches[2].Messages, 3)
		require.Len(t, batches[3].Messages, 1)
	})

	t.Run("BothLimitsSizeTriggersFirst", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(10, 100)
		batches := splitBatch(batch, 250, 5)

		// Size limit (250) triggers before count limit (5)
		require.Len(t, batches, 5, "should split into 5 batches")
		require.Len(t, batches[0].Messages, 2) // 200 bytes
		require.Len(t, batches[1].Messages, 2) // 200 bytes
		require.Len(t, batches[2].Messages, 2) // 200 bytes
		require.Len(t, batches[3].Messages, 2) // 200 bytes
		require.Len(t, batches[4].Messages, 2) // 200 bytes
	})

	t.Run("SingleLargeMessage", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(1, 1000)
		batches := splitBatch(batch, 500, 0)

		// Single message should not be split, even if it exceeds size limit
		require.Len(t, batches, 1, "should return single batch")
		require.Len(t, batches[0].Messages, 1, "batch should contain the large message")
	})

	t.Run("VariableMessageSizes", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = []parsers.Message{
			createMessage(0, 100),
			createMessage(1, 200),
			createMessage(2, 150),
		}
		batches := splitBatch(batch, 250, 0)

		require.Len(t, batches, 3, "should split into 3 batches")
		require.Len(t, batches[0].Messages, 1) // 100 bytes
		require.Len(t, batches[1].Messages, 1) // 200 bytes (100+200 > 250)
		require.Len(t, batches[2].Messages, 1) // 150 bytes (200+150 > 250)
	})

	t.Run("ManySmallMessages", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(1000, 10)
		batches := splitBatch(batch, 0, 500)

		require.Len(t, batches, 2, "should split into 2 batches")
		require.Len(t, batches[0].Messages, 500)
		require.Len(t, batches[1].Messages, 500)
	})

	t.Run("CountLimitStricterThanSize", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(5, 1000)
		batches := splitBatch(batch, 3000, 2)

		// Count limit (2) is stricter than size limit (3000)
		require.Len(t, batches, 3, "should split into 3 batches")
		require.Len(t, batches[0].Messages, 2) // 2000 bytes
		require.Len(t, batches[1].Messages, 2) // 2000 bytes
		require.Len(t, batches[2].Messages, 1) // 1000 bytes
	})

	t.Run("ExactBoundaryCountLimit", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(10, 100)
		batches := splitBatch(batch, 0, 10)

		require.Len(t, batches, 1, "should return single batch when count equals limit")
		require.Len(t, batches[0].Messages, 10)
	})

	t.Run("ExactBoundarySizeLimit", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(3, 100)
		batches := splitBatch(batch, 300, 0)

		require.Len(t, batches, 1, "should return single batch when size equals limit")
		require.Len(t, batches[0].Messages, 3)
	})

	t.Run("VerifyBatchOrder", func(t *testing.T) {
		batch := baseBatch
		batch.Messages = createMessages(10, 100)
		for i := range batch.Messages {
			batch.Messages[i].Offset = uint64(i)
		}

		batches := splitBatch(batch, 0, 3)

		// Verify messages are in correct order across batches
		messageIdx := 0
		for _, b := range batches {
			for _, msg := range b.Messages {
				require.Equal(t, uint64(messageIdx), msg.Offset, "messages should maintain order")
				messageIdx++
			}
		}
		require.Equal(t, 10, messageIdx, "all messages should be accounted for")
	})
}

func TestSplitBatchConsistentMetadata(t *testing.T) {
	topic := "test-topic"
	partition := uint32(42)

	batch := parsers.MessageBatch{
		Topic:     topic,
		Partition: partition,
		Messages:  createMessages(10, 100),
	}
	batches := splitBatch(batch, 0, 3)

	for i, b := range batches {
		require.Equal(t, topic, b.Topic, "batch %d should have correct topic", i)
		require.Equal(t, partition, b.Partition, "batch %d should have correct partition", i)
	}
}

func TestBatchSize(t *testing.T) {
	batch := parsers.MessageBatch{
		Topic:     "test",
		Partition: 0,
		Messages: []parsers.Message{
			createMessage(0, 100),
			createMessage(1, 200),
			createMessage(2, 50),
		},
	}

	size := batchSize(batch)
	require.Equal(t, uint64(350), size)
}

func createMessages(count int, size int) []parsers.Message {
	messages := make([]parsers.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = createMessage(i, size)
	}
	return messages
}

func createMessage(offset int, size int) parsers.Message {
	return parsers.Message{
		Offset: uint64(offset),
		Value:  make([]byte, size),
	}
}
