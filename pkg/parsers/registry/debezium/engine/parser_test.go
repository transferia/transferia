package engine

import (
	_ "embed"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/parsers"
)

var rawLines []string

//go:embed parser_test.jsonl
var parserTest []byte

func init() {
	rawLines = strings.Split(string(parserTest), "\n")
}

func makePersqueueReadMessage(i int, rawLine string) parsers.Message {
	return parsers.Message{
		Offset:     uint64(i),
		SeqNo:      0,
		Key:        []byte("test_source_id"),
		CreateTime: time.Now(),
		WriteTime:  time.Now(),
		Value:      []byte(rawLine),
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
		msg := makePersqueueReadMessage(0, line)
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
	msg := makePersqueueReadMessage(0, `{}`)
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
