//go:build !disable_kafka_provider

package kafka

import (
	"context"
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

type mockSink struct {
	pushF func([]abstract.ChangeItem) error
}

func (m mockSink) Close() error {
	return nil
}

func (m mockSink) AsyncPush(input []abstract.ChangeItem) chan error {
	logger.Log.Info("push begin")
	defer logger.Log.Info("push done")
	result := make(chan error, 1)
	go func() {
		result <- m.pushF(input)
	}()
	return result
}

func TestThrottler(t *testing.T) {
	reader := &mockKafkaReader{}
	readCh := make(chan struct{}, 1)
	sinker := &mockSink{
		pushF: func(items []abstract.ChangeItem) error {
			<-readCh
			return nil
		},
	}
	kafkaSource := &KafkaSource{BufferSize: 100}
	source, err := newSourceWithReader(kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), reader)
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

	defer closer.Close()
	for i := 0; i < 3; i++ {
		lgr.Infof("log item: %v", i)
	}
	time.Sleep(time.Second) // just in case

	src, err := NewSource("asd", kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
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
	_, err = NewSource("asd", kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
	kafkaSource.Topic = "topic1"
	kafkaClient, err := client.NewClient(kafkaSource.Connection.Brokers, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, kafkaSource.Topic, nil))
	_, err = NewSource("asd", kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
}

func TestNonExistsTopic(t *testing.T) {
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)
	kafkaSource.Topic = "tmp"
	_, err = NewSource("asd", kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
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

	defer closer.Close()
	for i := 0; i < 3; i++ {
		lgr.Infof("log item: %v", i)
	}
	time.Sleep(time.Second) // just in case

	kafkaSource.OffsetPolicy = AtStartOffsetPolicy // Will read old item (1, 2 and 3)
	src, err := NewSource("asd", kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
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
	src, err = NewSource("asd", kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	items, err = src.Fetch()
	require.NoError(t, err)
	src.Stop()
	abstract.Dump(items)
	require.Len(t, items, 2)
}
