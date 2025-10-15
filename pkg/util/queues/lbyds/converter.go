package lbyds

import (
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/parsers"
)

func ConvertBatches(batches []persqueue.MessageBatch) []parsers.MessageBatch {
	return yslices.Map(batches, func(t persqueue.MessageBatch) parsers.MessageBatch {
		return parsers.MessageBatch{
			Topic:     t.Topic,
			Partition: t.Partition,
			Messages: yslices.Map(t.Messages, func(t persqueue.ReadMessage) parsers.Message {
				return parsers.Message{
					Offset:     t.Offset,
					SeqNo:      t.SeqNo,
					Key:        t.SourceID,
					CreateTime: t.CreateTime,
					WriteTime:  t.WriteTime,
					Value:      t.Data,
					Headers:    t.ExtraFields,
				}
			}),
		}
	})
}
