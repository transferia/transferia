package kafka

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/providers/kafka/client"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	sourcehelpers "github.com/transferia/transferia/tests/helpers/source"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type mockKafkaReader struct {
	offset             int64
	maxCommittedOffset int64
}

func (m *mockKafkaReader) CommitMessages(_ context.Context, msgs ...kgo.Record) error {
	maxOffset := int64(0)
	for _, el := range msgs {
		if el.Offset > maxOffset {
			maxOffset = el.Offset
		}
	}
	m.maxCommittedOffset = maxOffset
	return nil
}

func (m *mockKafkaReader) FetchMessage(_ context.Context) (kgo.Record, error) {
	msg := kgo.Record{
		Topic:     "",
		Offset:    m.offset,
		Value:     []byte(strings.Repeat("a", 50)),
		Timestamp: time.Now(),
	}
	logger.Log.Infof("read msg: %v", m.offset)
	m.offset++
	return msg, nil
}

func (m *mockKafkaReader) Close() error {
	return nil
}

func TestThrottler(t *testing.T) {
	reader := &mockKafkaReader{}
	readCh := make(chan struct{}, 1)
	sinker := mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
		<-readCh
		return nil
	})
	kafkaSource := &KafkaSource{BufferSize: 100}
	source, err := newSource(kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	source.reader = reader
	require.NoError(t, err)
	require.True(t, source.inLimits())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, source.Run(sinker))
	}()
	time.Sleep(time.Second)
	require.False(t, source.inLimits())
	require.Equal(t, 2, int(reader.offset))
	require.Equal(t, int64(0), reader.maxCommittedOffset)
	readCh <- struct{}{}
	time.Sleep(time.Second)
	require.False(t, source.inLimits())
	require.Equal(t, int64(1), reader.maxCommittedOffset)
	require.Equal(t, 4, int(reader.offset))
	close(readCh)
	source.Stop()
	wg.Wait()
}

func TestConsumer(t *testing.T) {
	parserConfigMap, err := parsers.ParserConfigStructToMap(&jsonparser.ParserConfigJSONCommon{
		Fields:        []abstract.ColSchema{{ColumnName: "ts", DataType: "DateTime"}, {ColumnName: "msg", DataType: "string"}},
		AddRest:       false,
		AddDedupeKeys: true,
	})
	require.NoError(t, err)
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)
	kafkaSource.Topic = "topic1"
	kafkaSource.ParserConfig = parserConfigMap

	kafkaClient, err := client.NewClient(kafkaSource.Connection.Brokers, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, kafkaSource.Topic, nil))

	lgr, closer, err := logger.NewKafkaLogger(&logger.KafkaConfig{
		Broker:   kafkaSource.Connection.Brokers[0],
		Topic:    kafkaSource.Topic,
		User:     kafkaSource.Auth.User,
		Password: kafkaSource.Auth.Password,
	})
	require.NoError(t, err)

	defer func() { _ = closer.Close() }()
	for i := 0; i < 3; i++ {
		lgr.Infof("log item: %v", i)
	}
	time.Sleep(time.Second) // just in case

	src, err := NewSource("asd", kafkaSource, nil, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	items, err := src.Fetch()
	require.NoError(t, err)
	src.Stop()
	abstract.Dump(items)
}

func TestMissedTopic(t *testing.T) {
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)
	kafkaSource.Topic = "not-exists-topic"
	_, err = NewSource("asd", kafkaSource, nil, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
	kafkaSource.Topic = "topic1"
	kafkaClient, err := client.NewClient(kafkaSource.Connection.Brokers, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, kafkaSource.Topic, nil))
	_, err = NewSource("asd", kafkaSource, nil, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
}

func TestNonExistsTopic(t *testing.T) {
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)
	kafkaSource.Topic = "tmp"
	_, err = NewSource("asd", kafkaSource, nil, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.Error(t, err)
}

func TestOffsetPolicy(t *testing.T) {
	parserConfigMap, err := parsers.ParserConfigStructToMap(&jsonparser.ParserConfigJSONCommon{
		Fields:        []abstract.ColSchema{{ColumnName: "ts", DataType: "DateTime"}, {ColumnName: "msg", DataType: "string"}},
		AddRest:       false,
		AddDedupeKeys: true,
	})
	require.NoError(t, err)
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)
	kafkaSource.Topic = "topic2"
	kafkaSource.ParserConfig = parserConfigMap

	kafkaClient, err := client.NewClient(kafkaSource.Connection.Brokers, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, kafkaSource.Topic, nil))

	lgr, closer, err := logger.NewKafkaLogger(&logger.KafkaConfig{
		Broker:   kafkaSource.Connection.Brokers[0],
		Topic:    kafkaSource.Topic,
		User:     kafkaSource.Auth.User,
		Password: kafkaSource.Auth.Password,
	})
	require.NoError(t, err)

	defer func() { _ = closer.Close() }()
	for i := 0; i < 3; i++ {
		lgr.Infof("log item: %v", i)
	}
	time.Sleep(time.Second) // just in case

	kafkaSource.OffsetPolicy = AtStartOffsetPolicy // Will read old item (1, 2 and 3)
	src, err := NewSource("asd", kafkaSource, nil, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	items, err := src.Fetch()
	require.NoError(t, err)
	src.Stop()
	require.True(t, len(items) >= 3) // At least 3 old items
	abstract.Dump(items)

	go func() {
		time.Sleep(time.Second)
		for i := 3; i < 5; i++ {
			lgr.Infof("log item: %v", i)
		}
	}()

	kafkaSource.OffsetPolicy = AtEndOffsetPolicy // Will read only new items (3 and 4)
	src, err = NewSource("asd", kafkaSource, nil, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	items, err = src.Fetch()
	require.NoError(t, err)
	src.Stop()
	abstract.Dump(items)
	require.Len(t, items, 2)
}

type mockParser struct {
	parsers.Parser
}

func (m *mockParser) Do(_ parsers.Message, _ abstract.Partition) []abstract.ChangeItem {
	return []abstract.ChangeItem{{LSN: 0}}
}

func TestParseLSNNotSetNull(t *testing.T) {
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)

	kafkaSource.Topic = "topic2"
	src, err := NewSource("asd", kafkaSource, nil, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	src.parser = &mockParser{}

	parsedItems := src.parse([]kgo.Record{
		{
			Topic:     "topic2",
			Offset:    3,
			Value:     []byte("{\"ts\":\"2021-01-01T00:00:00Z\",\"msg\":\"test\"}"),
			Timestamp: time.Now(),
		},
	})

	require.Len(t, parsedItems, 1)
	require.Equal(t, uint64(3), parsedItems[0].LSN)
}

func TestPartitionSource(t *testing.T) {
	kafkaCfgTemplate, err := SourceRecipe()
	require.NoError(t, err)

	t.Run("FullyReadOnePartition", func(t *testing.T) {
		topicName := "fully_read_topic"
		topicPartition := int32(2)

		kafkaCfg := *kafkaCfgTemplate
		kafkaCfg.Topic = topicName
		partitionDesc := &PartitionDescription{
			Partition: topicPartition,
		}

		testData := createTopicAndFillWithData(t, topicName, &kafkaCfg)

		src, err := NewSource("dtt", &kafkaCfg, partitionDesc, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		result, err := sourcehelpers.WaitForItems(src, 10, 500*time.Millisecond)
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
		partitionDesc := &PartitionDescription{
			Partition: topicPartition,
		}

		testData := createTopicAndFillWithData(t, topicName, &kafkaCfg)

		// create tmp source and commit a few messages before run
		src, err := NewSource("dtt", &kafkaCfg, partitionDesc, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		require.NoError(t, src.reader.CommitMessages(context.Background(), []kgo.Record{
			{Topic: topicName, Partition: topicPartition, Offset: 0},
			{Topic: topicName, Partition: topicPartition, Offset: 1},
			{Topic: topicName, Partition: topicPartition, Offset: 2},
		}...))
		src.Stop()

		src, err = NewSource("dtt", &kafkaCfg, partitionDesc, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		result, err := sourcehelpers.WaitForItems(src, 7, 500*time.Millisecond)
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
