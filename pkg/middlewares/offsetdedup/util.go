package offsetdedup

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/queues/sequencer"
)

func positionOf(item abstract.ChangeItem) Position {
	return Position{
		Offset: item.QueueMessageMeta.Offset,
		Index:  item.QueueMessageMeta.Index,
	}
}

func sendResult(ctx context.Context, ackCh chan<- abstract.AsyncPushResult, result abstract.AsyncPushResult) {
	select {
	case ackCh <- result:
	case <-ctx.Done():
	}
}

func extractOffsetsFromChangeItems(items []abstract.ChangeItem) []uint64 {
	offsets := make([]uint64, len(items))
	for i := range items {
		offsets[i] = items[i].QueueMessageMeta.Offset
	}
	return offsets
}

func asyncPushResultOffsetsToRanges(result abstract.AsyncPushResult) string {
	queueResult, ok := result.GetResult().(abstract.QueueResult)
	if !ok {
		return ""
	}
	return sequencer.OffsetsToRanges(queueResult.Offsets)
}

func changeItemsOffsetsToRanges(items []abstract.ChangeItem) string {
	return sequencer.OffsetsToRanges(extractOffsetsFromChangeItems(items))
}

// holdBackTrailingOffsetGroup withholds all occurrences of the final offset.
// A later offset is required to prove that every ChangeItem produced from the
// preceding queue message has been processed.
func holdBackTrailingOffsetGroup(offsets []uint64) (readyToAcknowledge []uint64, awaitingNextOffset []uint64) {
	if len(offsets) == 0 {
		return nil, nil
	}

	trailingOffset := offsets[len(offsets)-1]
	trailingGroupStart := len(offsets) - 1
	for trailingGroupStart > 0 && offsets[trailingGroupStart-1] == trailingOffset {
		trailingGroupStart--
	}
	return offsets[:trailingGroupStart:trailingGroupStart], offsets[trailingGroupStart:]
}
