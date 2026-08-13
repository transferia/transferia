package offsetdedup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

var (
	errSentinel   = errors.New("sentinel error")
	testPartition = abstract.NewPartition("p1", 0)
	testTableID   = abstract.TableID{Namespace: "public", Name: "test_table"}
)

type offsetStoreStub struct {
	lastPosition *Position
	loadErr      error
	loadCalls    int
	loadedTable  abstract.TableID
}

func (s *offsetStoreStub) LoadLastPosition(_ context.Context, _ abstract.Partition, tableID abstract.TableID) (*Position, error) {
	s.loadCalls++
	s.loadedTable = tableID
	return s.lastPosition, s.loadErr
}

type sinkStub struct {
	pushed       [][]Position
	pushContexts []context.Context
	pushErr      error
	pushResults  [][]uint64
	deferResult  bool
	ackCh        chan<- abstract.AsyncPushResult
	pushStarted  chan struct{}
	releasePush  <-chan struct{}
	closeErr     error
	closeCalls   int
}

func (s *sinkStub) AsyncV2Push(ctx context.Context, ackCh chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	positions := make([]Position, len(items))
	offsets := make([]uint64, len(items))
	for i, item := range items {
		positions[i] = Position{Offset: item.QueueMessageMeta.Offset, Index: item.QueueMessageMeta.Index}
		offsets[i] = item.QueueMessageMeta.Offset
	}
	s.pushed = append(s.pushed, positions)
	s.pushContexts = append(s.pushContexts, ctx)
	s.ackCh = ackCh
	if s.pushStarted != nil {
		close(s.pushStarted)
	}
	if s.releasePush != nil {
		select {
		case <-s.releasePush:
		case <-ctx.Done():
			return
		}
	}
	if s.deferResult {
		return
	}

	if s.pushResults == nil {
		s.sendResult(ctx, offsets)
		return
	}
	for _, resultOffsets := range s.pushResults {
		s.sendResult(ctx, resultOffsets)
	}
}

func TestOffsetDedupBindsStreamContextOnFirstPush(t *testing.T) {
	sink := &sinkStub{}
	dd := newDedup(t, &offsetStoreStub{}, sink)
	streamCtx, cancelStream := context.WithCancel(context.Background())

	dd.AsyncV2Push(streamCtx, dd.ackCh, makeItems(testTableID, atOffsets(1, 2)))
	_, firstErr := readResult(t, dd.ackCh)
	dd.AsyncV2Push(streamCtx, dd.ackCh, makeItems(testTableID, atOffsets(2, 3)))
	_, secondErr := readResult(t, dd.ackCh)

	require.NoError(t, firstErr)
	require.NoError(t, secondErr)
	require.Len(t, sink.pushContexts, 2)
	require.Equal(t, dd.streamCtx, sink.pushContexts[0])
	require.Equal(t, sink.pushContexts[0], sink.pushContexts[1])

	cancelStream()
	require.Eventually(t, func() bool {
		select {
		case <-dd.streamCtx.Done():
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func (s *sinkStub) sendResult(ctx context.Context, offsets []uint64) {
	result := &abstract.QueueSourceAsyncPushResult{
		Result: abstract.QueueResult{Offsets: offsets},
		Err:    s.pushErr,
	}
	select {
	case s.ackCh <- result:
	case <-ctx.Done():
	}
}

func (s *sinkStub) Close() error {
	s.closeCalls++
	return s.closeErr
}

func atOffsets(offsets ...uint64) []Position {
	result := make([]Position, len(offsets))
	for i, offset := range offsets {
		result[i] = Position{Offset: offset}
	}
	return result
}

func makeItems(tableID abstract.TableID, positions []Position) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, len(positions))
	for i, position := range positions {
		result[i] = abstract.ChangeItem{
			Schema: tableID.Namespace,
			Table:  tableID.Name,
			QueueMessageMeta: changeitem.QueueMessageMeta{
				Offset: position.Offset,
				Index:  position.Index,
			},
		}
	}
	return result
}

type dedupHarness struct {
	*OffsetDedup
	ackCh chan abstract.AsyncPushResult
}

func newDedup(t *testing.T, store OffsetStore, sink abstract.QueueToS3Sink) *dedupHarness {
	t.Helper()
	dd := NewOffsetDedup(sink, store, testPartition, logger.Log)
	t.Cleanup(func() { _ = dd.Close() })
	return &dedupHarness{
		OffsetDedup: dd,
		ackCh:       make(chan abstract.AsyncPushResult, 16),
	}
}

func push(t *testing.T, dd *dedupHarness, positions []Position) ([]uint64, error) {
	t.Helper()
	return pushItems(t, dd, makeItems(testTableID, positions))
}

func pushItems(t *testing.T, dd *dedupHarness, items []abstract.ChangeItem) ([]uint64, error) {
	t.Helper()
	dd.AsyncV2Push(context.Background(), dd.ackCh, items)
	return readResult(t, dd.ackCh)
}

func readResult(t *testing.T, ackCh <-chan abstract.AsyncPushResult) ([]uint64, error) {
	t.Helper()
	result := <-ackCh
	queueResult, ok := result.GetResult().(abstract.QueueResult)
	require.True(t, ok, "unexpected result type %T", result.GetResult())
	return queueResult.Offsets, result.GetError()
}

func TestPushActionRequiresExactlyOneOutcome(t *testing.T) {
	result := abstract.NewQueueSourceAsyncPushResult(nil, nil)
	items := makeItems(testTableID, atOffsets(1))

	require.NotPanics(t, func() { newPushAction(items, nil) })
	require.NotPanics(t, func() { newPushAction(nil, result) })
	require.Panics(t, func() { newPushAction(nil, nil) })
	require.Panics(t, func() { newPushAction(items, result) })
}

func TestOffsetDedupNormal(t *testing.T) {
	testCases := []struct {
		name        string
		positions   []Position
		pushErr     error
		wantOffsets []uint64
	}{
		{
			name:        "strip last offset",
			positions:   atOffsets(1, 2, 3),
			wantOffsets: []uint64{1, 2},
		},
		{
			name:        "strip every item at last offset",
			positions:   atOffsets(1, 2, 2),
			wantOffsets: []uint64{1},
		},
		{
			name:        "propagate sink error",
			positions:   atOffsets(1, 2),
			pushErr:     errSentinel,
			wantOffsets: []uint64{1, 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &offsetStoreStub{}
			sink := &sinkStub{pushErr: tc.pushErr}
			dd := newDedup(t, store, sink)

			require.Zero(t, store.loadCalls, "watermark must be loaded lazily")
			gotOffsets, gotErr := push(t, dd, tc.positions)

			if tc.pushErr == nil {
				require.NoError(t, gotErr)
			} else {
				require.ErrorIs(t, gotErr, tc.pushErr)
			}
			require.Equal(t, tc.wantOffsets, gotOffsets)
			require.Equal(t, [][]Position{tc.positions}, sink.pushed)
			require.Equal(t, 1, store.loadCalls)
			require.Equal(t, testTableID, store.loadedTable)
		})
	}
}

func TestOffsetDedupCarriesUnackedOffsetsForward(t *testing.T) {
	sink := &sinkStub{}
	dd := newDedup(t, &offsetStoreStub{}, sink)

	dd.AsyncV2Push(context.Background(), dd.ackCh, makeItems(testTableID, atOffsets(42)))
	require.Eventually(t, func() bool {
		dd.mu.Lock()
		defer dd.mu.Unlock()
		return len(dd.offsetsAwaitingNextOffset) == 1 && dd.offsetsAwaitingNextOffset[0] == 42
	}, time.Second, time.Millisecond)
	require.Empty(t, dd.ackCh)

	dd.AsyncV2Push(context.Background(), dd.ackCh, makeItems(testTableID, atOffsets(42)))
	require.Eventually(t, func() bool {
		dd.mu.Lock()
		defer dd.mu.Unlock()
		return len(dd.offsetsAwaitingNextOffset) == 2 &&
			dd.offsetsAwaitingNextOffset[0] == 42 &&
			dd.offsetsAwaitingNextOffset[1] == 42
	}, time.Second, time.Millisecond)
	require.Empty(t, dd.ackCh, "the pending offset must remain unacked while it is still the last offset")

	dd.AsyncV2Push(context.Background(), dd.ackCh, makeItems(testTableID, atOffsets(43, 44)))
	gotOffsets, gotErr := readResult(t, dd.ackCh)
	require.NoError(t, gotErr)
	require.Equal(t, []uint64{42, 42, 43}, gotOffsets)
	require.Equal(t, [][]Position{atOffsets(42), atOffsets(42), atOffsets(43, 44)}, sink.pushed)
}

func TestOffsetDedupResultPump(t *testing.T) {
	t.Run("forward every result", func(t *testing.T) {
		sink := &sinkStub{pushResults: [][]uint64{{1, 2}, {3, 4, 5}, {6, 7}}}
		dd := newDedup(t, &offsetStoreStub{}, sink)

		pushDone := make(chan struct{})
		go func() {
			dd.AsyncV2Push(context.Background(), dd.ackCh, makeItems(testTableID, atOffsets(1, 2, 3, 4, 5, 6, 7)))
			close(pushDone)
		}()

		select {
		case <-pushDone:
		case <-time.After(time.Second):
			require.FailNow(t, "AsyncV2Push is blocked while the result pump is waiting for the state mutex")
		}

		firstOffsets, firstErr := readResult(t, dd.ackCh)
		secondOffsets, secondErr := readResult(t, dd.ackCh)
		thirdOffsets, thirdErr := readResult(t, dd.ackCh)
		require.NoError(t, firstErr)
		require.NoError(t, secondErr)
		require.NoError(t, thirdErr)
		require.Equal(t, []uint64{1}, firstOffsets)
		require.Equal(t, []uint64{2, 3, 4}, secondOffsets)
		require.Equal(t, []uint64{5, 6}, thirdOffsets)
	})

	t.Run("forward delayed result", func(t *testing.T) {
		sink := &sinkStub{deferResult: true}
		dd := newDedup(t, &offsetStoreStub{}, sink)

		dd.AsyncV2Push(context.Background(), dd.ackCh, makeItems(testTableID, atOffsets(1, 2, 3)))
		require.Empty(t, dd.ackCh)

		sink.sendResult(context.Background(), []uint64{1, 2, 3})
		gotOffsets, gotErr := readResult(t, dd.ackCh)
		require.NoError(t, gotErr)
		require.Equal(t, []uint64{1, 2}, gotOffsets)
	})
}

func TestOffsetDedupRejectsAnotherResultChannel(t *testing.T) {
	sink := &sinkStub{}
	dd := newDedup(t, &offsetStoreStub{}, sink)

	_, err := push(t, dd, atOffsets(1, 2))
	require.NoError(t, err)

	otherAckCh := make(chan abstract.AsyncPushResult, 1)
	dd.AsyncV2Push(context.Background(), otherAckCh, makeItems(testTableID, atOffsets(2)))
	_, err = readResult(t, otherAckCh)
	require.ErrorContains(t, err, "one result channel per stream")
	require.Equal(t, [][]Position{atOffsets(1, 2)}, sink.pushed)
}

func TestOffsetDedupSkipping(t *testing.T) {
	testCases := []struct {
		name        string
		saved       Position
		batches     [][]Position
		wantAcks    [][]uint64
		wantNextAck []uint64
		wantPushed  [][]Position
	}{
		{
			name:        "split batch at watermark",
			saved:       Position{Offset: 4},
			batches:     [][]Position{atOffsets(1, 4, 10, 11)},
			wantAcks:    [][]uint64{{1, 4, 10}},
			wantNextAck: []uint64{11, 20},
			wantPushed:  [][]Position{atOffsets(10, 11)},
		},
		{
			name:  "distinguish indexes within offset",
			saved: Position{Offset: 10, Index: 1},
			batches: [][]Position{{
				{Offset: 10, Index: 0},
				{Offset: 10, Index: 1},
				{Offset: 10, Index: 2},
				{Offset: 11, Index: 0},
			}},
			wantAcks:    [][]uint64{{10, 10, 10}},
			wantNextAck: []uint64{11, 20},
			wantPushed: [][]Position{{
				{Offset: 10, Index: 2},
				{Offset: 11, Index: 0},
			}},
		},
		{
			name:        "find watermark in non-monotonic batch",
			saved:       Position{Offset: 4},
			batches:     [][]Position{atOffsets(1, 2, 5, 3, 4)},
			wantAcks:    [][]uint64{{1, 2, 5, 3, 4}},
			wantNextAck: []uint64{20},
		},
		{
			name:        "skip across batches",
			saved:       Position{Offset: 4},
			batches:     [][]Position{atOffsets(1, 2), atOffsets(3, 5), atOffsets(4, 10)},
			wantAcks:    [][]uint64{{1, 2}, {3, 5}, {4}},
			wantNextAck: []uint64{10, 20},
			wantPushed:  [][]Position{atOffsets(10)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			saved := tc.saved
			store := &offsetStoreStub{lastPosition: &saved}
			sink := &sinkStub{}
			dd := newDedup(t, store, sink)

			for i, batch := range tc.batches {
				gotOffsets, gotErr := push(t, dd, batch)
				require.NoError(t, gotErr)
				require.Equal(t, tc.wantAcks[i], gotOffsets)
			}

			gotOffsets, gotErr := push(t, dd, atOffsets(20, 21))
			require.NoError(t, gotErr)
			require.Equal(t, tc.wantNextAck, gotOffsets, "push after watermark must use NORMAL mode")

			wantPushed := append([][]Position(nil), tc.wantPushed...)
			wantPushed = append(wantPushed, atOffsets(20, 21))
			require.Equal(t, wantPushed, sink.pushed)
			require.Equal(t, 1, store.loadCalls)
		})
	}
}

func TestOffsetDedupSkippingKeepsLastOffsetPending(t *testing.T) {
	saved := Position{Offset: 10, Index: 1}
	dd := newDedup(t, &offsetStoreStub{lastPosition: &saved}, &sinkStub{})
	positions := []Position{
		{Offset: 10, Index: 0},
		{Offset: 10, Index: 1},
		{Offset: 10, Index: 2},
	}

	dd.AsyncV2Push(context.Background(), dd.ackCh, makeItems(testTableID, positions))
	require.Eventually(t, func() bool {
		dd.mu.Lock()
		defer dd.mu.Unlock()
		return len(dd.offsetsAwaitingNextOffset) == 3
	}, time.Second, time.Millisecond)
	require.Empty(t, dd.ackCh, "offset 10 must not be acknowledged while it remains the last offset")

	gotOffsets, gotErr := push(t, dd, atOffsets(20, 21))
	require.NoError(t, gotErr)
	require.Equal(t, []uint64{10, 10, 10, 20}, gotOffsets)
}

func TestOffsetDedupSkippingPropagatesSinkError(t *testing.T) {
	saved := Position{Offset: 4}
	store := &offsetStoreStub{lastPosition: &saved}
	sink := &sinkStub{pushErr: errSentinel}
	dd := newDedup(t, store, sink)

	gotOffsets, gotErr := push(t, dd, atOffsets(1, 4, 10))

	require.ErrorIs(t, gotErr, errSentinel)
	require.Equal(t, []uint64{1, 4}, gotOffsets)
	require.Equal(t, [][]Position{atOffsets(10)}, sink.pushed)
}

func TestOffsetDedupInitialization(t *testing.T) {
	t.Run("empty batch is a no-op", func(t *testing.T) {
		store := &offsetStoreStub{}
		sink := &sinkStub{}
		dd := newDedup(t, store, sink)
		ackCh := make(chan abstract.AsyncPushResult, 1)

		dd.AsyncV2Push(context.Background(), ackCh, nil)

		require.Empty(t, ackCh)
		require.Empty(t, sink.pushed)
		require.Zero(t, store.loadCalls)
	})

	t.Run("retry after load error", func(t *testing.T) {
		store := &offsetStoreStub{loadErr: errSentinel}
		sink := &sinkStub{}
		dd := newDedup(t, store, sink)

		gotOffsets, gotErr := push(t, dd, atOffsets(1))
		require.ErrorIs(t, gotErr, errSentinel)
		require.Empty(t, gotOffsets)
		require.Empty(t, sink.pushed)

		store.loadErr = nil
		gotOffsets, gotErr = push(t, dd, atOffsets(1, 2))
		require.NoError(t, gotErr)
		require.Equal(t, []uint64{1}, gotOffsets)
		require.Equal(t, 2, store.loadCalls)
	})

	t.Run("load once and reject another table", func(t *testing.T) {
		store := &offsetStoreStub{}
		sink := &sinkStub{}
		dd := newDedup(t, store, sink)

		_, err := push(t, dd, atOffsets(1, 2))
		require.NoError(t, err)

		otherTable := abstract.TableID{Namespace: "public", Name: "other_table"}
		gotOffsets, gotErr := pushItems(t, dd, makeItems(otherTable, atOffsets(2)))
		require.ErrorContains(t, gotErr, "supports one table per stream")
		require.Empty(t, gotOffsets)
		require.Equal(t, 1, store.loadCalls)
		require.Equal(t, [][]Position{atOffsets(1, 2)}, sink.pushed)
	})
}

func TestOffsetDedupClose(t *testing.T) {
	store := &offsetStoreStub{}
	sink := &sinkStub{closeErr: errSentinel}
	dd := newDedup(t, store, sink)

	require.ErrorIs(t, dd.Close(), errSentinel)
	require.NoError(t, dd.Close())
	require.Equal(t, 1, sink.closeCalls)

	gotOffsets, gotErr := push(t, dd, atOffsets(1))
	require.ErrorIs(t, gotErr, abstract.AsyncPushConcurrencyErr)
	require.Empty(t, gotOffsets)
	require.Zero(t, store.loadCalls)
	require.Empty(t, sink.pushed)
}

func TestOffsetDedupCloseWaitsForInnerPush(t *testing.T) {
	pushStarted := make(chan struct{})
	releasePush := make(chan struct{})
	sink := &sinkStub{
		pushStarted: pushStarted,
		releasePush: releasePush,
	}
	dd := newDedup(t, &offsetStoreStub{}, sink)

	go dd.AsyncV2Push(context.Background(), dd.ackCh, makeItems(testTableID, atOffsets(1, 2)))
	<-pushStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- dd.Close()
	}()
	require.Eventually(t, func() bool {
		dd.mu.Lock()
		defer dd.mu.Unlock()
		return dd.closed
	}, time.Second, time.Millisecond)

	select {
	case <-closeDone:
		require.FailNow(t, "Close returned before the inner push completed")
	default:
	}

	close(releasePush)
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "Close did not finish after the inner push completed")
	}
	require.Equal(t, 1, sink.closeCalls)
}
