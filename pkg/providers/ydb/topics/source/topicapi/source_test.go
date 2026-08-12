package topicapisource

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/parsers"
	blankparser "github.com/transferia/transferia/pkg/parsers/registry/blank"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	"github.com/transferia/transferia/pkg/stats"
	sourcehelpers "github.com/transferia/transferia/tests/helpers/source"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	ydbtopic "github.com/transferia/transferia/tests/helpers/ydb_recipe/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestSource(t *testing.T) {
	sourceMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))

	t.Run("ReadOneTopic", func(t *testing.T) {
		topicName := "one_topic_test"
		testMessages := [][]byte{
			[]byte("message_1"),
			[]byte("message_2"),
			[]byte("message_3"),
		}
		ydbtopic.CreateAndFillTopic(t, topicName, testMessages)

		srcCfg := newTestSourceCfg(t, []string{topicName})
		parser, err := parsers.NewParserFromParserConfig(&blankparser.ParserConfigBlankLb{}, false, logger.Log, sourceMetrics)
		require.NoError(t, err)

		src, err := NewSource(srcCfg, parser, logger.Log, sourceMetrics)
		require.NoError(t, err)
		res, err := sourcehelpers.WaitForItems(src, len(testMessages), 0)
		require.NoError(t, err)

		checkItems(t, topicName, testMessages, res)
	})

	t.Run("ReadManyTopics", func(t *testing.T) {
		topicNameOne := "many_topics_test_1"
		testMessagesOne := [][]byte{[]byte("message_1_1"), []byte("message_1_2")}
		ydbtopic.CreateAndFillTopic(t, topicNameOne, testMessagesOne)

		topicNameTwo := "many_topics_test_2"
		testMessagesTwo := [][]byte{[]byte("message_2_1"), []byte("message_2_2")}
		ydbtopic.CreateAndFillTopic(t, topicNameTwo, testMessagesTwo)

		topicNameThree := "many_topics_test_3"
		testMessagesThree := [][]byte{[]byte("message_3_1"), []byte("message_3_2")}
		ydbtopic.CreateAndFillTopic(t, topicNameThree, testMessagesThree)

		srcCfg := newTestSourceCfg(t, []string{topicNameOne, topicNameTwo, topicNameThree})
		parser, err := parsers.NewParserFromParserConfig(&blankparser.ParserConfigBlankLb{}, false, logger.Log, sourceMetrics)
		require.NoError(t, err)

		src, err := NewSource(srcCfg, parser, logger.Log, sourceMetrics)
		require.NoError(t, err)
		res, err := sourcehelpers.WaitForItems(src, 6, 0)
		require.NoError(t, err)

		topicToResMessage := make(map[string][][]byte)
		for _, push := range res {
			for _, msg := range push {
				topicToResMessage[msg.Table] = append(topicToResMessage[msg.Table], msg.ColumnValues[7].([]byte))
			}
		}

		expectedTopicName := abstract.NewPartition(topicNameOne, 0).String()
		require.Len(t, topicToResMessage[expectedTopicName], len(testMessagesOne))
		for i, msg := range testMessagesOne {
			require.Equal(t, msg, topicToResMessage[expectedTopicName][i])
		}

		expectedTopicName = abstract.NewPartition(topicNameTwo, 0).String()
		require.Len(t, topicToResMessage[expectedTopicName], len(testMessagesTwo))
		for i, msg := range testMessagesTwo {
			require.Equal(t, msg, topicToResMessage[expectedTopicName][i])
		}

		expectedTopicName = abstract.NewPartition(topicNameThree, 0).String()
		require.Len(t, topicToResMessage[expectedTopicName], len(testMessagesThree))
		for i, msg := range testMessagesThree {
			require.Equal(t, msg, topicToResMessage[expectedTopicName][i])
		}
	})

	t.Run("ReadOneTopicWithUseFullPath", func(t *testing.T) {
		topicName := "some_dir/one_topic_test"
		testMessages := [][]byte{
			[]byte("message_1"),
			[]byte("message_2"),
			[]byte("message_3"),
		}
		ydbtopic.CreateAndFillTopic(t, topicName, testMessages)

		srcCfg := newTestSourceCfg(t, []string{topicName})
		srcCfg.UseFullTopicNameForParsing = true
		parser, err := parsers.NewParserFromParserConfig(&blankparser.ParserConfigBlankLb{}, false, logger.Log, sourceMetrics)
		require.NoError(t, err)

		src, err := NewSource(srcCfg, parser, logger.Log, sourceMetrics)
		require.NoError(t, err)
		res, err := sourcehelpers.WaitForItems(src, len(testMessages), 0)
		require.NoError(t, err)

		checkItems(t, topicName, testMessages, res)
	})
}

func TestSourceCodecs(t *testing.T) {
	sourceMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))

	ydbDriver := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbDriver.Close(context.Background())
	}()

	t.Run("EncodedWithGZIP", func(t *testing.T) {
		topicName := "gzip_topic_test"
		testMessages := [][]byte{
			[]byte("gzip_message_1"),
			[]byte("gzip_message_2"),
			[]byte("gzip_message_3"),
		}
		ydbtopic.CreateTopic(t, topicName, ydbDriver)
		ydbtopic.WriteMessages(t, topicName, testMessages, ydbDriver, topicoptions.WithWriterCodec(topictypes.CodecGzip))

		srcCfg := newTestSourceCfg(t, []string{topicName})
		parser, err := parsers.NewParserFromParserConfig(&blankparser.ParserConfigBlankLb{}, false, logger.Log, sourceMetrics)
		require.NoError(t, err)

		src, err := NewSource(srcCfg, parser, logger.Log, sourceMetrics)
		require.NoError(t, err)
		res, err := sourcehelpers.WaitForItems(src, len(testMessages), 0)
		require.NoError(t, err)

		checkItems(t, topicName, testMessages, res)
	})

	t.Run("EncodedWithZSTD", func(t *testing.T) {
		topicName := "zstd_topic_test"
		testMessages := [][]byte{
			[]byte("zstd_message_1"),
			[]byte("zstd_message_2"),
			[]byte("zstd_message_3"),
		}
		ydbtopic.CreateTopic(t, topicName, ydbDriver)
		ydbtopic.WriteMessages(t, topicName, testMessages, ydbDriver,
			topicoptions.WithWriterCodec(topictypes.CodecZstd),
			topicoptions.WithWriterAddEncoder(topictypes.CodecZstd, func(writer io.Writer) (io.WriteCloser, error) {
				return zstd.NewWriter(writer)
			}),
		)

		srcCfg := newTestSourceCfg(t, []string{topicName})
		parser, err := parsers.NewParserFromParserConfig(&blankparser.ParserConfigBlankLb{}, false, logger.Log, sourceMetrics)
		require.NoError(t, err)

		src, err := NewSource(srcCfg, parser, logger.Log, sourceMetrics)
		require.NoError(t, err)
		res, err := sourcehelpers.WaitForItems(src, len(testMessages), 0)
		require.NoError(t, err)

		checkItems(t, topicName, testMessages, res)
	})
}

func TestSourceFetch(t *testing.T) {
	topicName := "topic_fetch_test"
	testMessages := [][]byte{[]byte("message_1_1"), []byte("message_1_2")}
	ydbtopic.CreateAndFillTopic(t, topicName, testMessages)

	srcCfg := newTestSourceCfg(t, []string{topicName})
	sourceMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	src, err := NewSource(srcCfg, nil, logger.Log, sourceMetrics)
	require.NoError(t, err)
	defer src.Stop()

	res, err := src.Fetch()
	require.NoError(t, err)

	for idx, item := range res {
		require.Equal(t, topicName, item.Table)
		require.Equal(t, topicName, item.QueueMessageMeta.TopicName)
		require.Equal(t, string(testMessages[idx]), item.ColumnValues[4])
	}
}

func TestSourceCheckTopic(t *testing.T) {
	ydbClient := ydbrecipe.Driver(t)
	defer func() {
		_ = ydbClient.Close(context.Background())
	}()

	topicName := "absent_topic"

	sourceCfg := newTestSourceCfg(t, []string{topicName})
	sourceMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	_, err := NewSource(sourceCfg, nil, logger.Log, sourceMetrics)
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

func newTestSourceCfg(t *testing.T, topics []string) *topicsource.Config {
	instance, port, database, creds := ydbrecipe.InstancePortDatabaseCreds(t)
	return &topicsource.Config{
		Connection: topiccommon.ConnectionConfig{
			Endpoint:    fmt.Sprintf("%s:%d", instance, port),
			Database:    database,
			Credentials: creds,
		},
		Topics:   topics,
		Consumer: ydbtopic.DefaultConsumer,
	}
}

func checkItems(t *testing.T, topicName string, expectedMessages [][]byte, items [][]abstract.ChangeItem) {
	resLen := 0
	for _, push := range items {
		resLen += len(push)
	}
	require.Equal(t, len(expectedMessages), resLen)

	i := 0
	partitionName := abstract.NewPartition(topicName, 0).String()
	for _, push := range items {
		for _, item := range push {
			require.Equal(t, partitionName, item.Table)
			require.Equal(t, partitionName, item.ColumnValues[0])
			require.Equal(t, expectedMessages[i], item.ColumnValues[7])
			i++
		}
	}
}
