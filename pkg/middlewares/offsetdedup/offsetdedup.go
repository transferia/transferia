package offsetdedup

import (
	"context"
	"slices"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.QueueToS3Sink = (*OffsetDedup)(nil)

// OffsetDedup is a middleware between a queue source and a durable sink. Its
// purpose is to turn the sink's item-level durability into safe queue-offset
// acknowledgements and to avoid writing the same items twice after a restart.
//
// Ontology:
//
//   - Source is the owner of queue offsets. It sends ChangeItems through
//     AsyncV2Push and advances its committed offset only after receiving a
//     QueueResult from sourceResults.
//   - Sink is the wrapped QueueToS3Sink. It durably writes ChangeItems and
//     reports completed writes through sinkResults. It also owns persistence
//     of the latest durable Position exposed through OffsetStore.
//   - ChangeItem is one target-side event. Several adjacent ChangeItems may be
//     produced from one source message or WAL record.
//   - Partition is the source queue lane. Offsets and positions are meaningful
//     only inside their partition.
//   - Table is the destination relation identified by every ChangeItem. It is
//     part of the OffsetStore lookup scope because positions from different
//     tables must not be mixed.
//   - Offset identifies a source message within one partition. It is not a
//     unique ChangeItem identifier because one message may expand into several
//     items with the same offset.
//   - Index identifies a ChangeItem inside one offset. Position=(Offset, Index)
//     is therefore the unique restart watermark inside the bound partition and
//     table stream.
//   - OffsetStore is a read-only view of the sink's durable state. The sink
//     persists positions while OffsetDedup only loads the last one at startup.
//   - QueueResult is an acknowledgement candidate: its Offsets describe items
//     completed by the sink or skipped by this middleware. OffsetDedup may
//     delay or combine these offsets before returning them to the source.
//   - Stream is the lifetime shared by one partition, one table, one context,
//     and one source result channel. The first non-empty push binds these values
//     and loads the stream's last durable Position.
//
// Data flows in two directions:
//
//	Source -- ChangeItems --> OffsetDedup -- ChangeItems --> Sink
//	Source <-- QueueResult -- OffsetDedup <-- QueueResult -- Sink
//
// OffsetDedup has two processing modes:
//
//   - Recovery mode starts when OffsetStore returns a Position. Redelivered
//     ChangeItems are skipped through that exact position, including the item
//     at the position itself. If a batch also contains newer items, the skipped
//     offsets remain pending until those newer items succeed in the sink; only
//     then can both groups be acknowledged safely.
//   - Live mode starts immediately for a fresh stream, or after recovery finds
//     the saved Position. All items are forwarded to the sink. From every
//     successful sink result, the complete trailing group with the same offset
//     is withheld until a later offset arrives. Seeing a later offset proves
//     that all ChangeItems produced by the preceding source message have been
//     observed, so that preceding offset is safe to acknowledge.
//
// The mutable state mirrors those concepts: skipThrough is the recovery
// boundary, pendingSkippedOffsets waits for the first post-recovery sink
// result, and offsetsAwaitingNextOffset is the trailing live-stream group that
// is not yet safe to acknowledge.
type OffsetDedup struct {
	sink          abstract.QueueToS3Sink
	positionStore OffsetStore
	logger        log.Logger

	// stream lifetime context and cancel function
	streamCtx  context.Context
	stopStream context.CancelFunc

	partition     abstract.Partition
	sinkResults   chan abstract.AsyncPushResult
	sourceResults chan<- abstract.AsyncPushResult
	resultPumpWg  sync.WaitGroup
	activePushes  sync.WaitGroup

	mu                        sync.Mutex
	tableID                   *abstract.TableID
	skipThrough               *Position
	pendingSkippedOffsets     []uint64
	offsetsAwaitingNextOffset []uint64
	closed                    bool
}

func NewOffsetDedup(
	sink abstract.QueueToS3Sink,
	positionStore OffsetStore,
	partition abstract.Partition,
	lgr log.Logger,
) *OffsetDedup {
	loggerWithPartitionAndTopic := log.With(
		lgr,
		log.Int("partition", int(partition.Partition)),
		log.String("topic", partition.Topic),
		log.String("component", "offset_dedup"),
	)

	return &OffsetDedup{
		sink:                      sink,
		positionStore:             positionStore,
		logger:                    loggerWithPartitionAndTopic,
		partition:                 partition,
		streamCtx:                 nil,
		stopStream:                nil,
		sinkResults:               make(chan abstract.AsyncPushResult, 1),
		sourceResults:             nil,
		resultPumpWg:              sync.WaitGroup{},
		activePushes:              sync.WaitGroup{},
		mu:                        sync.Mutex{},
		tableID:                   nil,
		skipThrough:               nil,
		pendingSkippedOffsets:     nil,
		offsetsAwaitingNextOffset: nil,
		closed:                    false,
	}
}

func (o *OffsetDedup) forwardSinkResults() {
	defer o.resultPumpWg.Done()

	for {
		select {
		case <-o.streamCtx.Done():
			return
		case result, ok := <-o.sinkResults:
			if !ok {
				return
			}
			if result == nil {
				continue
			}
			o.logger.Debug("offset dedup received offsets from sink", log.String("offsets", asyncPushResultOffsetsToRanges(result)))
			modified := o.modifySinkResult(result)
			if modified != nil {
				o.logger.Debug("offset dedup sent offsets to source", log.String("offsets", asyncPushResultOffsetsToRanges(modified)))
				sendResult(o.streamCtx, o.sourceResults, modified)
			}
		}
	}
}

// modifySinkResult leaves the last offset uncommitted and, after skipping,
// prepends offsets skipped before the saved position.
func (o *OffsetDedup) modifySinkResult(result abstract.AsyncPushResult) abstract.AsyncPushResult {
	o.mu.Lock()
	defer o.mu.Unlock()

	skippedOffsets := o.pendingSkippedOffsets
	o.pendingSkippedOffsets = nil

	if result.GetError() != nil {
		if len(skippedOffsets) == 0 {
			return result
		}
		return abstract.NewQueueSourceAsyncPushResult(skippedOffsets, result.GetError())
	}

	queueResult, ok := result.GetResult().(abstract.QueueResult)
	if !ok {
		if len(skippedOffsets) == 0 {
			return result
		}
		return abstract.NewQueueSourceAsyncPushResult(skippedOffsets, nil)
	}

	candidateOffsets := slices.Concat(skippedOffsets, o.offsetsAwaitingNextOffset, queueResult.Offsets)
	readyToAcknowledge, awaitingNextOffset := holdBackTrailingOffsetGroup(candidateOffsets)
	o.offsetsAwaitingNextOffset = awaitingNextOffset

	if len(readyToAcknowledge) == 0 {
		return nil
	}
	return abstract.NewQueueSourceAsyncPushResult(readyToAcknowledge, nil)
}

// Close waits for in-flight operation, closes the inner sink, and stops the
// result pump after the inner sink can no longer publish acknowledgements.
func (o *OffsetDedup) Close() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	o.mu.Unlock()

	o.activePushes.Wait()
	err := o.sink.Close()
	if o.stopStream != nil {
		o.stopStream()
	}
	o.resultPumpWg.Wait()
	return err
}

// AsyncV2Push implements abstract.QueueToS3Sink. All non-empty pushes belong to
// one stream and must use the same context, result channel, and table. The first
// context becomes the stream lifetime context used by the wrapped sink.
func (o *OffsetDedup) AsyncV2Push(pushCtx context.Context, sourceResults chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	if len(items) == 0 {
		return
	}
	o.logger.Debug("offset dedup received offsets from source", log.String("offsets", changeItemsOffsetsToRanges(items)))

	action := o.routeBatch(pushCtx, sourceResults, items)
	if action.immediateResult != nil {
		o.logger.Debug("offset dedup sent offsets to source", log.String("offsets", asyncPushResultOffsetsToRanges(action.immediateResult)))
		sendResult(pushCtx, sourceResults, action.immediateResult)
		return
	}
	defer o.activePushes.Done()
	o.logger.Debug("offset dedup sent offsets to sink", log.String("offsets", changeItemsOffsetsToRanges(action.itemsToWrite)))
	o.sink.AsyncV2Push(o.streamCtx, o.sinkResults, action.itemsToWrite)
}

// routeBatch decides whether items are acknowledged immediately or written to
// sink. External calls remain in AsyncV2Push so they never run under mu.
func (o *OffsetDedup) routeBatch(
	streamCtx context.Context,
	sourceResults chan<- abstract.AsyncPushResult,
	items []abstract.ChangeItem,
) pushAction {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return newPushAction(nil, abstract.NewQueueSourceAsyncPushResult(nil, abstract.AsyncPushConcurrencyErr))
	}
	tableID := items[0].TableID()
	if err := o.lazyInitializeStream(streamCtx, sourceResults, tableID); err != nil {
		return newPushAction(nil, abstract.NewQueueSourceAsyncPushResult(nil, err))
	}

	var action pushAction
	if o.skipThrough == nil {
		action = newPushAction(items, nil)
	} else {
		action = o.skipCommittedItems(items)
	}

	if len(action.itemsToWrite) != 0 {
		o.activePushes.Add(1)
	}
	return action
}

// lazyInitializeStream binds the stream on its first batch and loads its
// restart position. Later calls verify the stream's immutable parameters. The
// caller must hold mu.
func (o *OffsetDedup) lazyInitializeStream(
	streamCtx context.Context,
	sourceResults chan<- abstract.AsyncPushResult,
	tableID abstract.TableID,
) error {
	if o.sourceResults == nil {
		o.streamCtx, o.stopStream = context.WithCancel(streamCtx)
		o.sourceResults = sourceResults
		o.resultPumpWg.Add(1)
		go o.forwardSinkResults()
	} else if o.sourceResults != sourceResults {
		return xerrors.New("offset dedup requires one result channel per stream")
	}

	if o.tableID == nil {
		skipThrough, err := o.positionStore.LoadLastPosition(o.streamCtx, o.partition, tableID)
		if err != nil {
			return xerrors.Errorf("load last position for table %s: %w", tableID.Fqtn(), err)
		}
		o.logger.Info("offset dedup loaded last position", log.Any("last_position", skipThrough), log.String("table", tableID.Fqtn()))
		o.tableID = &tableID
		o.skipThrough = skipThrough
	} else if *o.tableID != tableID {
		return xerrors.Errorf("offset dedup supports one table per stream: initialized for %s, got %s", o.tableID.Fqtn(), tableID.Fqtn())
	}
	return nil
}

// skipCommittedItems skips items through skipThrough. Once that position is
// found, the rest of the batch is written normally.
func (o *OffsetDedup) skipCommittedItems(items []abstract.ChangeItem) pushAction {
	boundaryIndex := -1
	for i, item := range items {
		if positionOf(item) == *o.skipThrough {
			boundaryIndex = i
			break
		}
	}

	if boundaryIndex == -1 {
		offsets := extractOffsetsFromChangeItems(items)
		o.logger.Debug("offset dedup skipped committed offsets", log.String("offsets", changeItemsOffsetsToRanges(items)))
		return newPushAction(nil, abstract.NewQueueSourceAsyncPushResult(offsets, nil))
	}

	skippedOffsets := extractOffsetsFromChangeItems(items[:boundaryIndex+1])
	o.logger.Debug("offset dedup skipped committed offsets", log.String("offsets", changeItemsOffsetsToRanges(items[:boundaryIndex+1])))
	itemsToWrite := items[boundaryIndex+1:]
	o.skipThrough = nil
	if len(itemsToWrite) == 0 {
		return newPushAction(nil, abstract.NewQueueSourceAsyncPushResult(skippedOffsets, nil))
	}

	o.pendingSkippedOffsets = skippedOffsets
	return newPushAction(itemsToWrite, nil)
}
