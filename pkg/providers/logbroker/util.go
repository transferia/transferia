package logbroker

import (
	"bytes"

	"github.com/transferia/transferia/pkg/abstract"
	serializer "github.com/transferia/transferia/pkg/serializer/queue"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func splitSerializedMessages(maxSize int, serializedMessages []serializer.SerializedMessage) (int, [][]topicwriter.Message) {
	var totalMessagesSize, currentBatchSize int
	currentBatch := make([]topicwriter.Message, 0)
	messageBatches := make([][]topicwriter.Message, 0)
	for idx, currSerializedMessage := range serializedMessages {
		currentBatchSize += len(currSerializedMessage.Value)
		currentBatch = append(currentBatch, topicwriter.Message{Data: bytes.NewReader(currSerializedMessage.Value)})

		if currentBatchSize >= maxSize || idx == len(serializedMessages)-1 {
			totalMessagesSize += currentBatchSize
			messageBatches = append(messageBatches, currentBatch)

			currentBatchSize = 0
			currentBatch = make([]topicwriter.Message, 0)
		}
	}

	return totalMessagesSize, messageBatches
}

func rearrangeTableToMessagesForMirror(tableToMessages map[abstract.TablePartID][]serializer.SerializedMessage) map[abstract.TablePartID][]serializer.SerializedMessage {
	newTableToMessages := make(map[abstract.TablePartID][]serializer.SerializedMessage)
	for _, msgArr := range tableToMessages {
		for _, msg := range msgArr {
			keyObject := abstract.TablePartID{
				TableID: abstract.TableID{
					Namespace: "",
					Name:      string(msg.Key),
				},
				PartID: "",
			}
			newTableToMessages[keyObject] = append(newTableToMessages[keyObject], msg)
		}
	}
	return newTableToMessages
}
