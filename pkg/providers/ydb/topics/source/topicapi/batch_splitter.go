package topicapisource

import "github.com/transferia/transferia/pkg/parsers"

// splitBatch cuts a single-partition batch into parts bounded by maxSize and
// maxMessageCount; a zero limit means unbounded. Always returns at least one
// batch, and a message larger than maxSize is emitted as its own batch.
func splitBatch(batch parsers.MessageBatch, maxSize uint32, maxMessageCount int) []parsers.MessageBatch {
	if maxSize == 0 && maxMessageCount == 0 {
		return []parsers.MessageBatch{batch}
	}

	if len(batch.Messages) == 0 {
		return []parsers.MessageBatch{batch}
	}

	var start int
	var currentSize uint32
	batches := make([]parsers.MessageBatch, 0)

	for i := 0; i < len(batch.Messages); i++ {
		msgSize := uint32(len(batch.Messages[i].Value))
		currentCount := i - start

		shouldSplit := false
		if maxMessageCount > 0 && currentCount >= maxMessageCount {
			shouldSplit = true
		}
		if maxSize > 0 && currentSize+msgSize > maxSize && currentCount > 0 {
			shouldSplit = true
		}

		if shouldSplit {
			batches = append(batches, parsers.MessageBatch{
				Topic:     batch.Topic,
				Partition: batch.Partition,
				Messages:  batch.Messages[start:i],
			})
			start = i
			currentSize = 0
		}

		currentSize += msgSize
	}

	// Last batch
	if start < len(batch.Messages) {
		batches = append(batches, parsers.MessageBatch{
			Topic:     batch.Topic,
			Partition: batch.Partition,
			Messages:  batch.Messages[start:],
		})
	}

	return batches
}

func batchSize(batch parsers.MessageBatch) uint64 {
	size := uint64(0)
	for i := range batch.Messages {
		size += uint64(len(batch.Messages[i].Value))
	}
	return size
}
