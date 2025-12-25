package logbroker

import "github.com/transferia/transferia/pkg/parsers"

type batch struct {
	Batches []parsers.MessageBatch
	commitF func()
}

func (b batch) Commit() {
	if b.commitF != nil {
		b.commitF()
	}
}

func newBatch(batches []parsers.MessageBatch) batch {
	return batch{
		Batches: batches,
		commitF: nil,
	}
}

func newBatches(maxSize int, commitF func(), batches []parsers.MessageBatch) []batch {
	// splits large MessageBatches into limited maxSize batches
	batchSizes := make([]int, 0, len(batches))
	splittedBatches := make([]parsers.MessageBatch, 0, len(batches))
	for _, batch := range batches {
		currBatchSize := 0
		currBatchBegIdx := 0
		for idx, msg := range batch.Messages {
			currBatchSize += len(msg.Value)
			if currBatchSize >= maxSize || idx == len(batch.Messages)-1 {
				splittedBatches = append(splittedBatches, parsers.MessageBatch{
					Topic:     batch.Topic,
					Partition: batch.Partition,
					Messages:  batch.Messages[currBatchBegIdx : idx+1],
				})

				batchSizes = append(batchSizes, currBatchSize)
				currBatchSize = 0
				currBatchBegIdx = idx + 1
			}
		}
	}

	// combines small MessageBatches
	currBatchSize := 0
	currCombinedBatches := []parsers.MessageBatch{}
	res := make([]batch, 0)
	for idx, batch := range splittedBatches {
		if currBatchSize+batchSizes[idx] > maxSize && len(currCombinedBatches) > 0 {
			res = append(res, newBatch(currCombinedBatches))

			currBatchSize = 0
			currCombinedBatches = []parsers.MessageBatch{}
		}

		currBatchSize += batchSizes[idx]
		currCombinedBatches = append(currCombinedBatches, batch)

		if idx == len(splittedBatches)-1 {
			res = append(res, newBatch(currCombinedBatches))
		}
	}
	res[len(res)-1].commitF = commitF

	return res
}
