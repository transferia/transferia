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
	topicID := path.Base(b.Topic)
	if len(topicID) == 0 || useFullTopicName {
		topicID = b.Topic
	}

	return abstract.MakeRawMessageWithMeta(
		m.Key,
		topicID,
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
	totalSize := 0
	st := time.Now()
	var data []abstract.ChangeItem
	for _, batch := range batches {
		for _, m := range batch.Messages {
			data = append(data, MessageAsChangeItem(m, batch, useFullTopicName))
			totalSize += len(m.Value)
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
		metrics.DecodeTime.RecordDuration(time.Since(st))
		logger.Debugf("Converter done in %v, %v rows", time.Since(st), len(data))
	}
	metrics.ChangeItems.Add(int64(len(data)))
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
