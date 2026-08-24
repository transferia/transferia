package mongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func txnOplogEntry(
	t *testing.T, ts primitive.Timestamp, lsidByte byte, txnNumber int64, o bson.D,
) oplogRsChangeEventV2 {
	lsidData := make([]byte, 16)
	lsidData[0] = lsidByte
	return decodeOplogEntry(t, bson.D{
		{Key: "lsid", Value: bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: lsidData}}}},
		{Key: "txnNumber", Value: txnNumber},
		{Key: "op", Value: "c"},
		{Key: "ns", Value: "admin.$cmd"},
		{Key: "o", Value: o},
		{Key: "ts", Value: ts},
		{Key: "v", Value: int64(2)},
	})
}

func insertOp(ns string, id any) bson.D {
	return bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: ns}, {Key: "o", Value: bson.D{{Key: "_id", Value: id}}}}
}

func deleteOp(ns string, id any) bson.D {
	return bson.D{{Key: "op", Value: "d"}, {Key: "ns", Value: ns}, {Key: "o", Value: bson.D{{Key: "_id", Value: id}}}}
}

func commitEntry(t *testing.T, ts primitive.Timestamp, lsidByte byte, txnNumber int64) oplogRsChangeEventV2 {
	return txnOplogEntry(t, ts, lsidByte, txnNumber, bson.D{
		{Key: "commitTransaction", Value: int32(1)},
		{Key: "commitTimestamp", Value: ts},
	})
}

func TestOplogTxnTracker_PreparedCommit(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	prevTS := primitive.Timestamp{T: 90, I: 1}
	prepareTS := primitive.Timestamp{T: 100, I: 1}
	commitTS := primitive.Timestamp{T: 105, I: 3}

	prepare := txnOplogEntry(t, prepareTS, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "a"), deleteOp("db1.coll1", "b")}},
		{Key: "prepare", Value: true},
		{Key: "count", Value: int64(2)},
	})
	events, size, err := tracker.Process(prepare, prevTS, 500)
	require.NoError(t, err)
	require.Empty(t, events)
	require.Zero(t, size)

	hold, held := tracker.HoldTS(prepareTS)
	require.True(t, held)
	require.Equal(t, prevTS, hold)

	events, size, err = tracker.Process(commitEntry(t, commitTS, 1, 5), prepareTS, 100)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, 600, size)
	require.Equal(t, "insert", events[0].OperationType)
	require.Equal(t, "delete", events[1].OperationType)
	for _, event := range events {
		require.Equal(t, commitTS, event.ClusterTime)
	}

	_, held = tracker.HoldTS(commitTS)
	require.False(t, held)
}

func TestOplogTxnTracker_PreparedAbort(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	prepareTS := primitive.Timestamp{T: 100, I: 1}

	prepare := txnOplogEntry(t, prepareTS, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{deleteOp("db1.coll1", "a")}},
		{Key: "prepare", Value: true},
	})
	events, _, err := tracker.Process(prepare, primitive.Timestamp{T: 90, I: 1}, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	abort := txnOplogEntry(t, primitive.Timestamp{T: 101, I: 1}, 1, 5, bson.D{
		{Key: "abortTransaction", Value: int32(1)},
	})
	events, _, err = tracker.Process(abort, prepareTS, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	_, held := tracker.HoldTS(primitive.Timestamp{T: 101, I: 1})
	require.False(t, held)
}

func TestOplogTxnTracker_UnpreparedImplicitCommit(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	ts := primitive.Timestamp{T: 100, I: 1}

	entry := txnOplogEntry(t, ts, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "a")}},
	})
	events, size, err := tracker.Process(entry, primitive.Timestamp{T: 90, I: 1}, 100)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, 100, size)
	require.Equal(t, "insert", events[0].OperationType)
	require.Equal(t, ts, events[0].ClusterTime)

	_, held := tracker.HoldTS(ts)
	require.False(t, held)
}

func TestOplogTxnTracker_PartialChainPrepared(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	prevTS := primitive.Timestamp{T: 90, I: 1}
	ts1 := primitive.Timestamp{T: 100, I: 1}
	ts2 := primitive.Timestamp{T: 100, I: 2}
	commitTS := primitive.Timestamp{T: 101, I: 1}

	partial := txnOplogEntry(t, ts1, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "a")}},
		{Key: "partialTxn", Value: true},
	})
	events, _, err := tracker.Process(partial, prevTS, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	prepare := txnOplogEntry(t, ts2, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "b")}},
		{Key: "prepare", Value: true},
		{Key: "count", Value: int64(2)},
	})
	events, _, err = tracker.Process(prepare, ts1, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	hold, held := tracker.HoldTS(ts2)
	require.True(t, held)
	require.Equal(t, prevTS, hold)

	events, size, err := tracker.Process(commitEntry(t, commitTS, 1, 5), ts2, 50)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, 250, size)
	require.Equal(t, "a", events[0].DocumentKey.ID)
	require.Equal(t, "b", events[1].DocumentKey.ID)
	for _, event := range events {
		require.Equal(t, commitTS, event.ClusterTime)
	}
}

func TestOplogTxnTracker_PartialChainImplicitCommit(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	ts1 := primitive.Timestamp{T: 100, I: 1}
	ts2 := primitive.Timestamp{T: 100, I: 2}

	partial := txnOplogEntry(t, ts1, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "a")}},
		{Key: "partialTxn", Value: true},
	})
	events, _, err := tracker.Process(partial, primitive.Timestamp{T: 90, I: 1}, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	final := txnOplogEntry(t, ts2, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "b")}},
		{Key: "count", Value: int64(2)},
	})
	events, _, err = tracker.Process(final, ts1, 100)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "a", events[0].DocumentKey.ID)
	require.Equal(t, "b", events[1].DocumentKey.ID)
	for _, event := range events {
		require.Equal(t, ts2, event.ClusterTime)
	}

	_, held := tracker.HoldTS(ts2)
	require.False(t, held)
}

func TestOplogTxnTracker_EmptyPreparedTxn(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	prepareTS := primitive.Timestamp{T: 100, I: 1}

	prepare := txnOplogEntry(t, prepareTS, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{}},
		{Key: "prepare", Value: true},
	})
	events, _, err := tracker.Process(prepare, primitive.Timestamp{T: 90, I: 1}, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	events, _, err = tracker.Process(commitEntry(t, primitive.Timestamp{T: 101, I: 1}, 1, 5), prepareTS, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	_, held := tracker.HoldTS(primitive.Timestamp{T: 101, I: 1})
	require.False(t, held)
}

func TestOplogTxnTracker_CommitOfUnknownTxnIsNotAnError(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	events, _, err := tracker.Process(commitEntry(t, primitive.Timestamp{T: 100, I: 1}, 1, 5), primitive.Timestamp{}, 100)
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestOplogTxnTracker_InterleavedSessions(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	t0 := primitive.Timestamp{T: 100, I: 1}
	t1 := primitive.Timestamp{T: 100, I: 2}
	t2 := primitive.Timestamp{T: 100, I: 3}
	t3 := primitive.Timestamp{T: 100, I: 4}
	t4 := primitive.Timestamp{T: 100, I: 5}

	prepareA := txnOplogEntry(t, t1, 1, 5, bson.D{
		{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "a")}},
		{Key: "prepare", Value: true},
	})
	events, _, err := tracker.Process(prepareA, t0, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	prepareB := txnOplogEntry(t, t2, 2, 7, bson.D{
		{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "b")}},
		{Key: "prepare", Value: true},
	})
	events, _, err = tracker.Process(prepareB, t1, 100)
	require.NoError(t, err)
	require.Empty(t, events)

	events, _, err = tracker.Process(commitEntry(t, t3, 2, 7), t2, 100)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "b", events[0].DocumentKey.ID)
	require.Equal(t, t3, events[0].ClusterTime)

	// A is still pending, progress is held at the entry before its prepare
	hold, held := tracker.HoldTS(t3)
	require.True(t, held)
	require.Equal(t, t0, hold)

	events, _, err = tracker.Process(commitEntry(t, t4, 1, 5), t3, 100)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "a", events[0].DocumentKey.ID)

	_, held = tracker.HoldTS(t4)
	require.False(t, held)
}

func TestOplogTxnTracker_PlainEntriesPassThrough(t *testing.T) {
	tracker := newOplogTxnTracker(logger.Log)
	ts := primitive.Timestamp{T: 100, I: 1}
	insert := decodeOplogEntry(t, bson.D{
		{Key: "ts", Value: ts},
		{Key: "v", Value: int64(2)},
		{Key: "op", Value: "i"},
		{Key: "ns", Value: "db1.coll1"},
		{Key: "o", Value: bson.D{{Key: "_id", Value: "a"}}},
	})
	events, size, err := tracker.Process(insert, primitive.Timestamp{T: 90, I: 1}, 100)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, 100, size)
	require.Equal(t, "insert", events[0].OperationType)

	// applyOps without lsid (manual admin command) is applied immediately
	manualApplyOps := decodeOplogEntry(t, bson.D{
		{Key: "ts", Value: ts},
		{Key: "v", Value: int64(2)},
		{Key: "op", Value: "c"},
		{Key: "ns", Value: "admin.$cmd"},
		{Key: "o", Value: bson.D{{Key: "applyOps", Value: bson.A{insertOp("db1.coll1", "b")}}}},
	})
	events, _, err = tracker.Process(manualApplyOps, ts, 100)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "b", events[0].DocumentKey.ID)

	// retryable write: a plain op with lsid/txnNumber must not be buffered
	retryable := decodeOplogEntry(t, bson.D{
		{Key: "lsid", Value: bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
		{Key: "txnNumber", Value: int64(3)},
		{Key: "stmtId", Value: int32(0)},
		{Key: "ts", Value: ts},
		{Key: "v", Value: int64(2)},
		{Key: "op", Value: "d"},
		{Key: "ns", Value: "db1.coll1"},
		{Key: "o", Value: bson.D{{Key: "_id", Value: "r"}}},
	})
	events, _, err = tracker.Process(retryable, ts, 100)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "delete", events[0].OperationType)
	require.Equal(t, "r", events[0].DocumentKey.ID)

	_, held := tracker.HoldTS(ts)
	require.False(t, held)
}
