package ydbtopic

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	ydb_topicwriter "github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

const DefaultConsumer = "dtt_consumer"

func CreateAndFillTopic(t *testing.T, topicName string, messages [][]byte) {
	CreateAndFillTopicWithDriver(t, topicName, messages, ydbrecipe.Driver(t))
}

func CreateAndFillTopicWithDriver(t *testing.T, topicName string, messages [][]byte, driver *ydb_go_sdk.Driver) {
	CreateTopic(t, topicName, driver)
	WriteMessages(t, topicName, messages, driver)
}

func CreateTopic(t *testing.T, topicName string, driver *ydb_go_sdk.Driver, createOpts ...topicoptions.CreateOption) {
	finalCreateOpts := append(createOpts, topicoptions.CreateWithConsumer(topictypes.Consumer{
		Name:            DefaultConsumer,
		SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
	}))

	require.NoError(t, driver.Topic().Create(context.Background(), topicName, finalCreateOpts...))
}

func WriteMessages(t *testing.T, topicName string, messages [][]byte, driver *ydb_go_sdk.Driver, wrOptions ...topicoptions.WriterOption) {
	topicMessages := make([]ydb_topicwriter.Message, len(messages))
	for i := range messages {
		topicMessages[i] = ydb_topicwriter.Message{Data: bytes.NewReader(messages[i])}
	}

	WriteTopicMessages(t, topicName, topicMessages, driver, wrOptions...)
}

func WriteTopicMessages(t *testing.T, topicName string, messages []ydb_topicwriter.Message, driver *ydb_go_sdk.Driver, wrOptions ...topicoptions.WriterOption) {
	wr, err := driver.Topic().StartWriter(topicName,
		append(wrOptions, topicoptions.WithWriterWaitServerAck(true))...,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	for _, msg := range messages {
		require.NoError(t, wr.Write(ctx, msg))
	}
	require.NoError(t, wr.Close(ctx))
}
