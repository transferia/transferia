package engine

import (
	_ "embed"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	confluentsrmock "github.com/transferia/transferia/tests/helpers/confluent_schema_registry_mock"
)

var rawLines []string

//go:embed parser_test.jsonl
var parserTest []byte

func init() {
	rawLines = strings.Split(string(parserTest), "\n")
}

func makePersqueueReadMessage(i int, rawBytes []byte) parsers.Message {
	return parsers.Message{
		Offset:     uint64(i),
		SeqNo:      0,
		Key:        []byte("test_source_id"),
		CreateTime: time.Now(),
		WriteTime:  time.Now(),
		Value:      rawBytes,
		Headers:    map[string]string{"some_field": "test"},
	}
}

func TestParser(t *testing.T) {
	var canonArr []interface{}
	for _, line := range rawLines {
		if line == "" {
			continue
		}
		parser := NewDebeziumImpl(logger.Log, nil, 1)
		msg := makePersqueueReadMessage(0, []byte(line))
		result := parser.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		canonArr = append(canonArr, result[0])
		fmt.Println(result[0].ToJSONString())
		abstract.Dump(result)
	}
	canon.SaveJSON(t, canonArr)
}

func TestUnparsed(t *testing.T) {
	parser := NewDebeziumImpl(logger.Log, nil, 1)
	msg := makePersqueueReadMessage(0, []byte(`{}`))
	result := parser.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: "my-topic-name"})
	require.Len(t, result, 1)
	fmt.Println(result[0].ToJSONString())
	require.Equal(t, "my-topic-name_unparsed", result[0].Table)
}

func TestMultiThreading(t *testing.T) {
	messages := make([]parsers.Message, 0)
	for i := 0; i < 100; i++ {
		for _, line := range rawLines {
			messages = append(messages, parsers.Message{
				Offset:     uint64(i),
				SeqNo:      0,
				Key:        []byte("test_source_id"),
				CreateTime: time.Now(),
				WriteTime:  time.Now(),
				Value:      []byte(line),
				Headers:    nil,
			})
		}
	}
	batch := parsers.MessageBatch{
		Topic:     "topicName",
		Partition: 0,
		Messages:  messages,
	}
	parserOneThread := NewDebeziumImpl(logger.Log, nil, 1)
	changeItemsSingleThread := parserOneThread.DoBatch(batch)

	parserMultiThread := NewDebeziumImpl(logger.Log, nil, 8)
	changeItemsMultiThread := parserMultiThread.DoBatch(batch)
	require.Equal(t, changeItemsSingleThread, changeItemsMultiThread)
}

func TestIncorrectMagicByte(t *testing.T) {
	schemaRegistryMock := confluentsrmock.NewConfluentSRMock(nil, nil)
	defer schemaRegistryMock.Close()

	client, err := confluent.NewSchemaRegistryClientWithTransport("", "", logger.Log)
	require.NoError(t, err)
	parser := NewDebeziumImpl(logger.Log, client, 1)

	for i := 1; i < 255; i++ {
		buf := make([]byte, 0)
		buf = append(buf, byte(i))
		buf = append(buf, []byte("\u0000\u0000\u0000{}")...)

		result := parser.Do(makePersqueueReadMessage(0, buf), abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		require.True(t, strings.HasSuffix(result[0].Table, "_unparsed"))
	}
}
