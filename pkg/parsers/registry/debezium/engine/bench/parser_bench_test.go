package main

import (
	_ "embed"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/debezium/engine"
)

//go:embed parser_test.jsonl
var benchTest []byte

func BenchmarkParsingViaMultithreading(b *testing.B) {
	batchSizes := []int{1, 10, 100, 1000, 10000, 100000}
	b.ResetTimer()
	for _, size := range batchSizes {
		for threads := 1; threads <= runtime.NumCPU()*8; threads *= 2 {
			parser := engine.NewDebeziumImpl(logger.Log, nil, uint64(threads))
			batch := makeBenchBatch(size)
			b.ResetTimer()
			b.Run(fmt.Sprintf("process %d messages in batch in %d threads", size, threads), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					parser.DoBatch(batch)
					runtime.GC()
				}
				// works only for go version 1.20+
				//	NsForMessage := float64(b.Elapsed()/(time.Nanosecond)) / float64(size*b.N)
				//	b.ReportMetric(NsForMessage, "ns/message")
				//	b.ReportMetric(1/NsForMessage*float64(time.Second), "messages/s")
				//	b.ReportMetric(0, "ns/op")
			})
		}
	}
}

func makeBenchBatch(size int) parsers.MessageBatch {
	messages := make([]parsers.Message, 0)
	for len(messages) < size {
		messages = append(messages, parsers.Message{
			Offset:     uint64(len(messages)),
			SeqNo:      0,
			Key:        []byte("test_source_id"),
			CreateTime: time.Now(),
			WriteTime:  time.Now(),
			Value:      benchTest,
			Headers:    nil,
		})
	}
	return parsers.MessageBatch{
		Topic:     "topicName",
		Partition: 0,
		Messages:  messages,
	}
}
