package lbyds

import (
	"fmt"
	"path"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func ChangeItemAsMessage(ci abstract.ChangeItem) (parsers.Message, abstract.Partition) {
	partition := ci.ColumnValues[1].(int)
	seqNo := ci.ColumnValues[2].(uint64)
	wTime := ci.ColumnValues[3].(time.Time)
	var data []byte
	switch v := ci.ColumnValues[4].(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	}

	var headers map[string]string
	if rawHeaders, ok := ci.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageMeta]].(map[string]string); ok {
		headers = rawHeaders
	}

	var key []byte
	if rawKey, ok := ci.ColumnValues[abstract.RawDataColsIDX[abstract.RawSequenceKey]].([]byte); ok {
		key = rawKey
	}

	return parsers.Message{
			Offset:     ci.LSN,
			SeqNo:      seqNo,
			Key:        key,
			CreateTime: time.Unix(0, int64(ci.CommitTime)),
			WriteTime:  wTime,
			Value:      data,
			Headers:    headers,
		}, abstract.Partition{
			Cluster:   "", // v1 protocol does not contains such entity
			Partition: uint32(partition),
			Topic:     ci.Table,
		}
}

func MessageAsChangeItem(m parsers.Message, b parsers.MessageBatch, useFullTopicName bool) abstract.ChangeItem {
	return abstract.MakeRawMessageWithMeta(
		m.Key,
		topicFromTopicPath(b.Topic, useFullTopicName),
		m.WriteTime,
		b.Topic,
		int(b.Partition),
		int64(m.Offset),
		m.Value,
		m.Headers,
	)
}

type TransformFunc func([]abstract.ChangeItem) []abstract.ChangeItem

func Parse(batches []parsers.MessageBatch, parser parsers.Parser, metrics *stats.SourceStats, logger log.Logger, transformFunc TransformFunc, useFullTopicName bool) []abstract.ChangeItem {
	st := time.Now()
	var data []abstract.ChangeItem
	if transformFunc == nil && parser != nil {
		data = parse(batches, parser, useFullTopicName)
	} else {
		data = parseWithTransform(batches, parser, transformFunc, useFullTopicName)
	}

	metrics.DecodeTime.RecordDuration(time.Since(st))
	metrics.ChangeItems.Add(int64(len(data)))
	logger.Debugf("Converter done in %v, %v rows", time.Since(st), len(data))

	for _, ci := range data {
		if ci.IsRowEvent() {
			if parsers.IsUnparsed(ci) {
				metrics.Unparsed.Inc()
			} else {
				metrics.Parsed.Inc()
			}
		}
	}
	return data
}

// BuildMapPartitionToLbOffsetsRange - is used only in logging
func BuildMapPartitionToLbOffsetsRange(v []parsers.MessageBatch) map[string][]uint64 {
	partitionToLbOffsetsRange := make(map[string][]uint64)
	for _, b := range v {
		partition := fmt.Sprintf("%v@%v", b.Topic, b.Partition)
		partitionToLbOffsetsRange[partition] = make([]uint64, 0)

		if len(b.Messages) == 1 {
			partitionToLbOffsetsRange[partition] = append(partitionToLbOffsetsRange[partition], b.Messages[0].Offset)
		} else if len(b.Messages) > 1 {
			partitionToLbOffsetsRange[partition] = append(partitionToLbOffsetsRange[partition], b.Messages[0].Offset)
			partitionToLbOffsetsRange[partition] = append(partitionToLbOffsetsRange[partition], b.Messages[len(b.Messages)-1].Offset)
		}
	}
	return partitionToLbOffsetsRange
}

func topicFromTopicPath(topicName string, useFullTopicName bool) string {
	topic := path.Base(topicName)
	if len(topic) == 0 || useFullTopicName {
		topic = topicName
	}
	return topic
}

func parse(batches []parsers.MessageBatch, parser parsers.Parser, useFullTopicName bool) []abstract.ChangeItem {
	batches = combineBatches(batches)

	var data []abstract.ChangeItem
	for _, b := range batches {

		b.Topic = topicFromTopicPath(b.Topic, useFullTopicName)
		data = append(data, parser.DoBatch(b)...)
	}

	return data
}

func combineBatches(batches []parsers.MessageBatch) []parsers.MessageBatch {
	batchesOrder := make([]int, 0)
	topicPartitionMessages := make(map[string]map[uint32][]parsers.Message)
	for idx, b := range batches {
		if _, ok := topicPartitionMessages[b.Topic]; !ok {
			topicPartitionMessages[b.Topic] = make(map[uint32][]parsers.Message)
		}
		if _, ok := topicPartitionMessages[b.Topic][b.Partition]; !ok {
			batchesOrder = append(batchesOrder, idx)
		}

		topicPartitionMessages[b.Topic][b.Partition] = append(topicPartitionMessages[b.Topic][b.Partition], b.Messages...)
	}

	res := make([]parsers.MessageBatch, 0, len(batchesOrder))
	for _, idx := range batchesOrder {
		topic := batches[idx].Topic
		partition := batches[idx].Partition

		res = append(res, parsers.MessageBatch{
			Topic:     topic,
			Partition: partition,
			Messages:  topicPartitionMessages[topic][partition],
		})
	}

	return res
}

func parseWithTransform(batches []parsers.MessageBatch, parser parsers.Parser, transformFunc TransformFunc, useFullTopicName bool) []abstract.ChangeItem {
	var data []abstract.ChangeItem
	for _, batch := range batches {
		for _, m := range batch.Messages {
			data = append(data, MessageAsChangeItem(m, batch, useFullTopicName))
		}
	}
	if transformFunc != nil {
		data = transformFunc(data)
	}
	if parser != nil {
		var res []abstract.ChangeItem
		for _, row := range data {
			changeItem, partition := ChangeItemAsMessage(row)
			res = append(res, parser.Do(changeItem, partition)...)
		}
		data = res
	}

	return data
}
