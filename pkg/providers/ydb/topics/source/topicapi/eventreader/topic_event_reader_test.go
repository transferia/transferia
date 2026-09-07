package eventreader

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader/event"
	"github.com/transferia/transferia/pkg/util/set"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

const (
	partitionsCount = 3
	readingTimeout  = 3 * time.Second
)

type partitionEvents struct {
	readStartEvent bool
	msgs           [][]byte
}

func TestReadOneTopicWithoutCommit(t *testing.T) {
	ctx := context.Background()
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicPath := "test_read_topic"
	consumer := createTopic(t, ydbClient, topicPath)

	messageCountPerPartition := 50
	partitionsTestData := make([][][]byte, partitionsCount)
	for p := range partitionsTestData {
		partitionsTestData[p] = make([][]byte, messageCountPerPartition)
		for i := range partitionsTestData[p] {
			partitionsTestData[p][i] = []byte(fmt.Sprintf("val_%d_%d", p, i))
		}
	}
	writeMessages(t, ydbClient, topicPath, partitionsTestData)

	t.Run("Read all messages", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
		require.NoError(t, err)
		defer func() { require.NoError(t, rd.Close(ctx)) }()

		topicToPartitions, _ := readAll(t, rd, set.New(topicPath), messageCountPerPartition*partitionsCount)
		partitionsData, ok := topicToPartitions[topicPath]
		require.True(t, ok)
		for i, p := range partitionsData {
			require.True(t, p.readStartEvent)
			require.Equal(t, partitionsTestData[i], p.msgs)
		}
	})

	t.Run("Read all messages again", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
		require.NoError(t, err)

		topicToPartitions, _ := readAll(t, rd, set.New(topicPath), messageCountPerPartition*partitionsCount)
		partitionsData, ok := topicToPartitions[topicPath]
		require.True(t, ok)
		for i, p := range partitionsData {
			require.True(t, p.readStartEvent)
			require.Equal(t, partitionsTestData[i], p.msgs)
		}
		require.NoError(t, rd.Close(ctx))
	})
}

func TestReadAndCommitOneTopic(t *testing.T) {
	ctx := context.Background()
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicPath := "test_read_commit_topic"
	consumer := createTopic(t, ydbClient, topicPath)

	messageCountPerPartition := 10
	partitionsTestData := make([][][]byte, partitionsCount)
	for p := range partitionsTestData {
		partitionsTestData[p] = make([][]byte, messageCountPerPartition)
		for i := range partitionsTestData[p] {
			partitionsTestData[p][i] = []byte(fmt.Sprintf("val_%d_%d", p, i))
		}
	}
	writeMessages(t, ydbClient, topicPath, partitionsTestData)

	t.Run("Read all messages and commit", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
		require.NoError(t, err)
		defer func() { require.NoError(t, rd.Close(ctx)) }()

		topicToPartitions, batches := readAll(t, rd, set.New(topicPath), messageCountPerPartition*partitionsCount)
		partitionsData, ok := topicToPartitions[topicPath]
		require.True(t, ok)
		for i, p := range partitionsData {
			require.True(t, p.readStartEvent)
			require.Equal(t, partitionsTestData[i], p.msgs)
		}

		for _, batch := range batches {
			batch.Commit()
		}
		time.Sleep(1 * time.Second)
	})

	t.Run("Try to read committed messages", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
		require.NoError(t, err)
		defer func() { require.NoError(t, rd.Close(ctx)) }()

		for {
			nextEvent, err := nextEventWithTimeout(rd.NextEvent)
			if err != nil {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Nil(t, nextEvent)
				break
			}
			require.NoError(t, err)
			_, ok := nextEvent.(*event.ReadEvent)
			require.False(t, ok)

			if startEvent, ok := nextEvent.(*event.StartEvent); ok {
				require.Equal(t, startEvent.PartitionInfo.LastOffset, int64(messageCountPerPartition))
			}
		}
	})

	// write new messages
	partitionsTestData = make([][][]byte, partitionsCount)
	for p := range partitionsTestData {
		partitionsTestData[p] = make([][]byte, messageCountPerPartition)
		for i := range partitionsTestData[p] {
			partitionsTestData[p][i] = []byte(fmt.Sprintf("val_%d_%d", p, messageCountPerPartition+i))
		}
	}
	writeMessages(t, ydbClient, topicPath, partitionsTestData)

	t.Run("Read new messages", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
		require.NoError(t, err)
		defer func() { require.NoError(t, rd.Close(ctx)) }()

		topicToPartitions, _ := readAll(t, rd, set.New(topicPath), messageCountPerPartition*partitionsCount)
		partitionsData, ok := topicToPartitions[topicPath]
		require.True(t, ok)
		for i, p := range partitionsData {
			require.True(t, p.readStartEvent)
			require.Equal(t, partitionsTestData[i], p.msgs)
		}
	})
}

func TestReadAndCommitManyTopics(t *testing.T) {
	ctx := context.Background()
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicPaths := []string{
		"test_read_commit_many_topics_first",
		"test_read_commit_many_topics_second",
		"test_read_commit_many_topics_third",
	}
	consumer := "many_topics_consumer"
	for i := range topicPaths {
		createTopicWithConsumer(t, ydbClient, consumer, topicPaths[i])
	}

	messageCountPerPartition := 10
	topicPartitionsTestData := make(map[string][][][]byte, len(topicPaths))
	for _, topicPath := range topicPaths {
		partitionsTestData := make([][][]byte, partitionsCount)
		for p := range partitionsTestData {
			partitionsTestData[p] = make([][]byte, messageCountPerPartition)
			for i := range partitionsTestData[p] {
				partitionsTestData[p][i] = []byte(fmt.Sprintf("val_%s_%d_%d", topicPath, p, i))
			}
		}
		writeMessages(t, ydbClient, topicPath, partitionsTestData)
		topicPartitionsTestData[topicPath] = partitionsTestData
	}

	t.Run("Read and commit all topic's messages", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, topicPaths, ydbClient, logger.Log)
		require.NoError(t, err)
		defer func() { require.NoError(t, rd.Close(ctx)) }()

		topicToPartitions, batches := readAll(t, rd, set.New(topicPaths...), messageCountPerPartition*partitionsCount*len(topicPaths))
		for _, topicPath := range topicPaths {
			partitionsData, ok := topicToPartitions[topicPath]
			require.True(t, ok)
			partitionsTestData, ok := topicPartitionsTestData[topicPath]
			require.True(t, ok)
			for i, p := range partitionsData {
				require.True(t, p.readStartEvent)
				require.Equal(t, partitionsTestData[i], p.msgs)
			}
		}

		for _, batch := range batches {
			batch.Commit()
		}
		time.Sleep(1 * time.Second)
	})

	t.Run("Try to read committed messages", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, topicPaths, ydbClient, logger.Log)
		require.NoError(t, err)
		defer func() { require.NoError(t, rd.Close(ctx)) }()

		for {
			nextEvent, err := nextEventWithTimeout(rd.NextEvent)
			if err != nil {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Nil(t, nextEvent)
				break
			}
			require.NoError(t, err)
			_, ok := nextEvent.(*event.ReadEvent)
			require.False(t, ok)

			if startEvent, ok := nextEvent.(*event.StartEvent); ok {
				require.Equal(t, startEvent.PartitionInfo.LastOffset, int64(messageCountPerPartition))
			}
		}
	})
}

func TestReaderClose(t *testing.T) {
	ctx := context.Background()
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicPath := "test_read_commit_topic"
	consumer := createTopic(t, ydbClient, topicPath)

	messageCountPerPartition := 10
	partitionsTestData := make([][][]byte, partitionsCount)
	for p := range partitionsTestData {
		partitionsTestData[p] = make([][]byte, messageCountPerPartition)
		for i := range partitionsTestData[p] {
			partitionsTestData[p][i] = []byte(fmt.Sprintf("val_%d_%d", p, i))
		}
	}
	writeMessages(t, ydbClient, topicPath, partitionsTestData)

	t.Run("Close reader", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
		require.NoError(t, err)
		require.NoError(t, rd.Close(ctx))

		isClosed := false
		select {
		case <-rd.stopCh:
			isClosed = true
		default:
		}
		require.True(t, isClosed)
	})

	t.Run("Close reader twice", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
		require.NoError(t, err)
		require.NoError(t, rd.Close(ctx))
		require.NoError(t, rd.Close(ctx))
	})

	t.Run("Read event after close", func(t *testing.T) {
		rd, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
		require.NoError(t, err)
		require.NoError(t, rd.Close(ctx))
		_, err = rd.NextEvent(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrClosedReader)
	})
}

func TestReceiveEventsOnAddingNewConsumerInGroup(t *testing.T) {
	ctx := context.Background()
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicPath := "test_get_stop_event_topic"
	consumer := createTopic(t, ydbClient, topicPath)

	// first reader starts consumption all partitions
	firstReader, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
	require.NoError(t, err)

	for i := 0; i < partitionsCount; i++ {
		nextEvent, err := nextEventWithTimeout(firstReader.NextEvent)
		require.NoError(t, err)
		require.NotNil(t, nextEvent)
		require.IsType(t, &event.StartEvent{}, nextEvent)

		startEv := nextEvent.(*event.StartEvent)
		startEv.Confirm()
	}

	// second reader joins consumption and causes rebalancing
	secondReader, err := NewTopicEventReader(consumer, []string{topicPath}, ydbClient, logger.Log)
	require.NoError(t, err)
	defer func() { require.NoError(t, secondReader.Close(ctx)) }()

	// first reader receives events about stopping reading of some partitions
	var stopEvents []*event.StopEvent
	for {
		nextEvent, err := nextEventWithTimeout(firstReader.NextEvent)
		if err != nil && xerrors.Is(err, context.DeadlineExceeded) {
			break
		}

		require.NoError(t, err)
		require.NotNil(t, nextEvent)
		stopEvent, ok := nextEvent.(*event.StopEvent)
		require.True(t, ok)
		stopEvents = append(stopEvents, stopEvent)
	}
	require.NotEqual(t, 0, len(stopEvents))

	// second reader tries to start reading, but it isn't possible because first reader hasn't confirmed it yet
	_, err = nextEventWithTimeout(secondReader.NextEvent)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// first reader confirms stop reading
	firstReaderPartitionsStop := set.New[int64]()
	for _, nextEvent := range stopEvents {
		nextEvent.Confirm()
		firstReaderPartitionsStop.Add(nextEvent.PartitionInfo.PartitionID)
	}

	// second reader receives events about the beginning of reading those partitions that the first worker stopped reading
	secondReaderPartitionsStart := set.New[int64]()
	for i := 0; i < firstReaderPartitionsStop.Len(); i++ {
		nextEvent, err := nextEventWithTimeout(secondReader.NextEvent)
		require.NoError(t, err)
		require.NotNil(t, nextEvent)
		startEvent, ok := nextEvent.(*event.StartEvent)
		require.True(t, ok)
		require.True(t, firstReaderPartitionsStop.Contains(startEvent.PartitionInfo.PartitionID))
		secondReaderPartitionsStart.Add(startEvent.PartitionInfo.PartitionID)
	}

	// first reader closes and the second one should receive events about reading all the partitions
	require.NoError(t, firstReader.Close(ctx))
	secondReaderPartitionsStartAfterClose := set.New[int64]()
	for {
		nextEvent, err := nextEventWithTimeout(secondReader.NextEvent)
		if err != nil && xerrors.Is(err, context.DeadlineExceeded) {
			break
		}

		require.NoError(t, err)
		require.NotNil(t, nextEvent)
		startEvent, ok := nextEvent.(*event.StartEvent)
		require.True(t, ok)
		secondReaderPartitionsStartAfterClose.Add(startEvent.PartitionInfo.PartitionID)
	}

	require.Equal(t, partitionsCount, secondReaderPartitionsStart.Len()+secondReaderPartitionsStartAfterClose.Len())
	for i := 0; i < partitionsCount; i++ {
		require.True(t, secondReaderPartitionsStart.Contains(int64(i)) || secondReaderPartitionsStartAfterClose.Contains(int64(i)))
	}
}

func readAll(t *testing.T, rd *TopicEventReader, topicPaths *set.Set[string], expectedMsgCount int) (map[string][]*partitionEvents, []*event.ReadEvent) {
	readBatches := make([]*event.ReadEvent, 0)
	topicToReadPartitionEvents := make(map[string][]*partitionEvents)

	readValueCount := 0
	for readValueCount < expectedMsgCount {
		nextEvent, err := nextEventWithTimeout(rd.NextEvent)
		require.NoError(t, err)
		switch typedEvent := nextEvent.(type) {
		case *event.StartEvent:
			require.True(t, topicPaths.Contains(typedEvent.PartitionInfo.TopicPath))

			logger.Log.Infof("received StartPartitionReading event: %v", typedEvent.PartitionInfo)
			_, ok := topicToReadPartitionEvents[typedEvent.PartitionInfo.TopicPath]
			if !ok {
				topicToReadPartitionEvents[typedEvent.PartitionInfo.TopicPath] = emptyPartitionEvents()
			}
			pe := topicToReadPartitionEvents[typedEvent.PartitionInfo.TopicPath]
			pe[typedEvent.PartitionInfo.PartitionID].readStartEvent = true

			typedEvent.Confirm()

		case *event.ReadEvent:
			require.NotNil(t, typedEvent.Batch.Messages)
			require.NotEmpty(t, typedEvent.Batch.Messages)

			logger.Log.Infof("received messages batch, topic: %s, partition: %d, size: %d",
				typedEvent.Batch.Topic, typedEvent.Batch.Partition, len(typedEvent.Batch.Messages))
			readBatches = append(readBatches, typedEvent)
			readValueCount += len(typedEvent.Batch.Messages)
			for _, m := range typedEvent.Batch.Messages {
				pe, ok := topicToReadPartitionEvents[typedEvent.Batch.Topic]
				require.True(t, ok, "Partitions messages received before StartReadingPartition event")
				pe[typedEvent.Batch.Partition].msgs = append(pe[typedEvent.Batch.Partition].msgs, m.Value)
			}

		case *event.StopEvent:

		default:
			t.Error("wrong event type received")
		}
	}

	return topicToReadPartitionEvents, readBatches
}

func emptyPartitionEvents() []*partitionEvents {
	readPartitions := make([]*partitionEvents, 0, partitionsCount)
	for i := 0; i < partitionsCount; i++ {
		readPartitions = append(readPartitions, &partitionEvents{
			readStartEvent: false,
			msgs:           make([][]byte, 0),
		})
	}
	return readPartitions
}

func nextEventWithTimeout(readFunc func(ctx context.Context) (event.Event, error)) (event.Event, error) {
	readCtx, cancel := context.WithTimeout(context.Background(), readingTimeout)
	defer cancel()
	return readFunc(readCtx)
}

func createTopic(t *testing.T, db *ydb.Driver, topicPath string) string {
	consumer := path.Base(topicPath) + "_consumer"
	return createTopicWithConsumer(t, db, consumer, topicPath)
}

func createTopicWithConsumer(t *testing.T, db *ydb.Driver, consumer, topicPath string) string {
	require.NoError(t, db.Topic().Create(
		context.Background(),
		topicPath,
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumer}),
		topicoptions.CreateWithMinActivePartitions(partitionsCount),
		topicoptions.CreateWithMaxActivePartitions(partitionsCount),
	))

	return consumer
}

func writeMessages(t *testing.T, db *ydb.Driver, topicPath string, values [][][]byte) {
	require.True(t, len(values) <= partitionsCount,
		"Values slice count should be <= partition count")

	for p := range values {
		ctx := context.Background()
		wr, err := db.Topic().StartWriter(
			topicPath,
			topicoptions.WithWriterPartitionID(int64(p)),
			topicoptions.WithWriterWaitServerAck(true),
		)
		require.NoError(t, err)

		for _, v := range values[p] {
			require.NoError(t, wr.Write(
				ctx,
				topicwriter.Message{
					Data: bytes.NewReader(v),
				}),
			)
		}
		require.NoError(t, wr.Close(ctx))
	}
}
