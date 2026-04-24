package event

import (
	"context"
	"io"

	"github.com/transferia/transferia/pkg/parsers"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
)

const ipMetaKey = "_ip"

type ReadEvent struct {
	Batch parsers.MessageBatch

	commit func(ctx context.Context) error
}

func (e *ReadEvent) Commit(ctx context.Context) error {
	return e.commit(ctx)
}

func (e *ReadEvent) isEvent() {}

func NewReadEvent(event *topiclistener.ReadMessages) (*ReadEvent, error) {
	messages := make([]parsers.Message, 0, len(event.Batch.Messages))
	for _, msg := range event.Batch.Messages {
		value, err := io.ReadAll(msg)
		if err != nil {
			return nil, err
		}

		messages = append(messages, parsers.Message{
			Offset:     uint64(msg.Offset),
			Key:        []byte(msg.ProducerID),
			Value:      value,
			CreateTime: msg.CreatedAt,
			WriteTime:  msg.CreatedAt,
			Headers:    combineMetadata(msg.Metadata, msg.WriteSessionMetadata),
			SeqNo:      uint64(msg.SeqNo),
		})
	}

	return &ReadEvent{
		Batch: parsers.MessageBatch{
			Topic:     event.PartitionSession.TopicPath,
			Partition: uint32(event.PartitionSession.PartitionID),
			Messages:  messages,
		},
		commit: event.ConfirmWithAck,
	}, nil
}

func combineMetadata(userMeta map[string][]byte, writeMeta map[string]string) map[string]string {
	if userMeta == nil && writeMeta == nil {
		return nil
	}

	metadata := make(map[string]string, len(userMeta)+len(writeMeta))
	for k, v := range userMeta {
		metadata[k] = string(v)
	}
	for k, v := range writeMeta {
		if k == ipMetaKey {
			continue
		}
		metadata[k] = v
	}

	return metadata
}
