package ydbtopic

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

const DefaultConsumer = "dtt_consumer"

func CreateAndFillTopic(t *testing.T, topicName string, messages [][]byte) {
	CreateAndFillTopicWithDriver(t, topicName, messages, ydbrecipe.Driver(t))
}

func CreateAndFillTopicWithDriver(t *testing.T, topicName string, messages [][]byte, driver *ydb.Driver) {
	createTopic(t, topicName, driver)
	writeMessages(t, topicName, messages, driver)
}

func createTopic(t *testing.T, topicName string, driver *ydb.Driver) {
	require.NoError(t, driver.Topic().Create(context.Background(), topicName, topicoptions.CreateWithConsumer(topictypes.Consumer{
		Name:            DefaultConsumer,
		SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
	})))
}

func writeMessages(t *testing.T, topicName string, messages [][]byte, driver *ydb.Driver) {
	wr, err := driver.Topic().StartWriter(topicName, topicoptions.WithWriterWaitServerAck(true))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	for _, msg := range messages {
		require.NoError(t, wr.Write(ctx, topicwriter.Message{Data: bytes.NewReader(msg)}))
	}
	require.NoError(t, wr.Close(ctx))
}
