package mongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func decodeOplogEntry(t *testing.T, entry bson.D) oplogRsChangeEventV2 {
	raw, err := bson.Marshal(entry)
	require.NoError(t, err)
	var result oplogRsChangeEventV2
	require.NoError(t, bson.Unmarshal(raw, &result))
	return result
}

func TestOplogV2Parser_ApplyOpsTransaction(t *testing.T) {
	txnTS := primitive.Timestamp{T: 1786692402, I: 1155}
	invoiceID := bson.D{{Key: "namespace", Value: "taxi"}, {Key: "id", Value: "27e918179d76ccf4b71499fbd8426364"}}
	projectionID := primitive.NewObjectID()

	entry := decodeOplogEntry(t, bson.D{
		{Key: "lsid", Value: bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
		{Key: "txnNumber", Value: int64(6529)},
		{Key: "op", Value: "c"},
		{Key: "ns", Value: "admin.$cmd"},
		{Key: "o", Value: bson.D{{Key: "applyOps", Value: bson.A{
			bson.D{
				{Key: "op", Value: "u"},
				{Key: "ns", Value: "payment-platform.invoices"},
				{Key: "ui", Value: primitive.Binary{Subtype: 4, Data: make([]byte, 16)}},
				{Key: "o", Value: bson.D{
					{Key: "$v", Value: int32(2)},
					{Key: "diff", Value: bson.D{{Key: "u", Value: bson.D{{Key: "version", Value: int32(34)}}}}},
				}},
				{Key: "o2", Value: bson.D{{Key: "_id", Value: invoiceID}}},
			},
			bson.D{
				{Key: "op", Value: "u"},
				{Key: "ns", Value: "payment-platform.transaction_projections"},
				{Key: "o", Value: bson.D{
					{Key: "$v", Value: int32(2)},
					{Key: "diff", Value: bson.D{{Key: "u", Value: bson.D{{Key: "status", Value: "captured"}}}}},
				}},
				{Key: "o2", Value: bson.D{{Key: "invoice_id", Value: invoiceID}, {Key: "_id", Value: projectionID}}},
			},
			bson.D{
				{Key: "op", Value: "i"},
				{Key: "ns", Value: "payment-platform.events"},
				{Key: "o", Value: bson.D{{Key: "_id", Value: "evt-1"}, {Key: "payload", Value: "x"}}},
			},
			bson.D{
				{Key: "op", Value: "d"},
				{Key: "ns", Value: "payment-platform.events"},
				{Key: "o", Value: bson.D{{Key: "_id", Value: "evt-0"}}},
			},
		}}}},
		{Key: "ts", Value: txnTS},
		{Key: "t", Value: int64(83)},
		{Key: "v", Value: int64(2)},
		{Key: "wall", Value: primitive.NewDateTimeFromTime(FromMongoTimestamp(txnTS))},
		{Key: "prevOpTime", Value: bson.D{{Key: "ts", Value: primitive.Timestamp{}}, {Key: "t", Value: int64(-1)}}},
	})

	events, err := entry.toMongoKeyChangeEvents(logger.Log)
	require.NoError(t, err)
	require.Len(t, events, 4)

	require.Equal(t, "update", events[0].OperationType)
	require.Equal(t, Namespace{Database: "payment-platform", Collection: "invoices"}, events[0].Namespace)
	require.Equal(t, invoiceID, events[0].DocumentKey.ID)

	require.Equal(t, "update", events[1].OperationType)
	require.Equal(t, Namespace{Database: "payment-platform", Collection: "transaction_projections"}, events[1].Namespace)
	require.Equal(t, projectionID, events[1].DocumentKey.ID)

	require.Equal(t, "insert", events[2].OperationType)
	require.Equal(t, Namespace{Database: "payment-platform", Collection: "events"}, events[2].Namespace)
	require.Equal(t, "evt-1", events[2].DocumentKey.ID)

	require.Equal(t, "delete", events[3].OperationType)
	require.Equal(t, Namespace{Database: "payment-platform", Collection: "events"}, events[3].Namespace)
	require.Equal(t, "evt-0", events[3].DocumentKey.ID)

	for _, event := range events {
		require.Equal(t, txnTS, event.ClusterTime)
	}
}

func TestOplogV2Parser_ApplyOpsPreparedAndPartial(t *testing.T) {
	ts := primitive.Timestamp{T: 300, I: 2}
	entry := decodeOplogEntry(t, bson.D{
		{Key: "ts", Value: ts},
		{Key: "v", Value: int64(2)},
		{Key: "op", Value: "c"},
		{Key: "ns", Value: "admin.$cmd"},
		{Key: "o", Value: bson.D{
			{Key: "applyOps", Value: bson.A{
				bson.D{
					{Key: "op", Value: "i"},
					{Key: "ns", Value: "db1.coll1"},
					{Key: "o", Value: bson.D{{Key: "_id", Value: "a"}}},
				},
				bson.D{{Key: "op", Value: "n"}, {Key: "ns", Value: ""}, {Key: "o", Value: bson.D{}}},
			}},
			{Key: "prepare", Value: true},
			{Key: "partialTxn", Value: true},
			{Key: "count", Value: int64(2)},
		}},
		{Key: "prevOpTime", Value: bson.D{
			{Key: "ts", Value: primitive.Timestamp{T: 300, I: 1}},
			{Key: "t", Value: int64(1)},
		}},
	})
	events, err := entry.toMongoKeyChangeEvents(logger.Log)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "insert", events[0].OperationType)
	require.Equal(t, "a", events[0].DocumentKey.ID)
	require.Equal(t, ts, events[0].ClusterTime)
}

func TestOplogV2Parser_TransactionControlEntriesAreSkipped(t *testing.T) {
	for _, command := range []string{"commitTransaction", "abortTransaction"} {
		entry := decodeOplogEntry(t, bson.D{
			{Key: "ts", Value: primitive.Timestamp{T: 100, I: 1}},
			{Key: "v", Value: int64(2)},
			{Key: "op", Value: "c"},
			{Key: "ns", Value: "admin.$cmd"},
			{Key: "o", Value: bson.D{
				{Key: command, Value: int32(1)},
				{Key: "commitTimestamp", Value: primitive.Timestamp{T: 100, I: 1}},
			}},
		})
		events, err := entry.toMongoKeyChangeEvents(logger.Log)
		require.NoError(t, err, command)
		require.Empty(t, events, command)
	}
}

func TestOplogV2Parser_PlainEntries(t *testing.T) {
	ts := primitive.Timestamp{T: 200, I: 7}

	insert := decodeOplogEntry(t, bson.D{
		{Key: "ts", Value: ts},
		{Key: "v", Value: int64(2)},
		{Key: "op", Value: "i"},
		{Key: "ns", Value: "db1.coll1"},
		{Key: "o", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "a", Value: "b"}}},
	})
	events, err := insert.toMongoKeyChangeEvents(logger.Log)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "insert", events[0].OperationType)
	require.Equal(t, Namespace{Database: "db1", Collection: "coll1"}, events[0].Namespace)
	require.Equal(t, int32(1), events[0].DocumentKey.ID)
	require.Equal(t, ts, events[0].ClusterTime)

	noop := decodeOplogEntry(t, bson.D{
		{Key: "ts", Value: ts},
		{Key: "v", Value: int64(2)},
		{Key: "op", Value: "n"},
		{Key: "ns", Value: ""},
		{Key: "o", Value: bson.D{{Key: "msg", Value: "periodic noop"}}},
	})
	events, err = noop.toMongoKeyChangeEvents(logger.Log)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "noop", events[0].OperationType)
	require.Equal(t, ts, events[0].ClusterTime)

	create := decodeOplogEntry(t, bson.D{
		{Key: "ts", Value: ts},
		{Key: "v", Value: int64(2)},
		{Key: "op", Value: "c"},
		{Key: "ns", Value: "db1.$cmd"},
		{Key: "o", Value: bson.D{{Key: "create", Value: "coll2"}}},
	})
	events, err = create.toMongoKeyChangeEvents(logger.Log)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "create", events[0].OperationType)
	require.Equal(t, Namespace{Database: "db1", Collection: "coll2"}, events[0].Namespace)
}

func TestMakeOplogShouldReplicate_SystemDatabases(t *testing.T) {
	allCollections := makeOplogShouldReplicate(&MongoSource{
		Collections: nil,
		ExcludedCollections: []MongoCollection{
			{DatabaseName: "payment-platform", CollectionName: "transaction_projections"},
		},
	})
	require.True(t, allCollections("noop", Namespace{}))
	require.True(t, allCollections("update", Namespace{Database: "payment-platform", Collection: "invoices"}))
	require.False(t, allCollections("update", MakeNamespace("payment-platform", "transaction_projections")))
	require.False(t, allCollections("update", MakeNamespace(DataTransferSystemDatabase, ClusterTimeCollName)))
	for _, systemDB := range SystemDBs {
		require.False(t, allCollections("update", MakeNamespace(systemDB, "system.sessions")), systemDB)
		require.False(t, allCollections("insert", MakeNamespace(systemDB, "cache.chunks.db1.coll1")), systemDB)
		require.False(t, allCollections("dropDatabase", MakeNamespace(systemDB, "")), systemDB)
	}

	explicitCollections := makeOplogShouldReplicate(&MongoSource{
		Collections: []MongoCollection{{DatabaseName: "payment-platform", CollectionName: "*"}},
	})
	require.True(t, explicitCollections("update", Namespace{Database: "payment-platform", Collection: "invoices"}))
	require.False(t, explicitCollections("update", Namespace{Database: "other", Collection: "invoices"}))
	require.False(t, explicitCollections("update", Namespace{Database: "config", Collection: "system.sessions"}))
}
