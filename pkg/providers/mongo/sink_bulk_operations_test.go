package mongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"go.mongodb.org/mongo-driver/bson"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
)

// retryTestSinker makes a sinker over the recipe mongo and a dropped collection with a unique index on "u".
func retryTestSinker(t *testing.T, ctx context.Context, collection string) (*sinker, *mongo_driver.Collection, Namespace) {
	return testSinker(t, ctx, collection, true)
}

// testSinker makes a sinker over the recipe mongo and a dropped collection, with a unique index on "u" on request.
func testSinker(
	t *testing.T, ctx context.Context, collection string, uniqueIndex bool,
) (*sinker, *mongo_driver.Collection, Namespace) {
	target := RecipeTarget()
	client, err := Connect(ctx, target.ConnectionOptions(nil), logger.Log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	namespace := Namespace{Database: "test_db", Collection: collection}
	coll := client.Database(namespace.Database).Collection(namespace.Collection)
	require.NoError(t, coll.Drop(ctx))
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	if uniqueIndex {
		_, err = coll.Indexes().CreateOne(ctx, mongo_driver.IndexModel{
			Keys:    bson.D{{Key: "u", Value: 1}},
			Options: mongo_options.Index().SetUnique(true),
		})
		require.NoError(t, err)
	}

	return &sinker{
		client:  client,
		logger:  logger.Log,
		metrics: stats.NewSinkerStats(solomon.NewRegistry(nil)),
		config:  target,
	}, coll, namespace
}

func upsertItem(id string, doc bson.D) abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  DocumentSchema.ColumnsNames,
		ColumnValues: []interface{}{id, doc},
		TableSchema:  DocumentSchema.Columns,
	}
}

func replaceItem(id string, doc bson.D) abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:         abstract.UpdateKind,
		ColumnNames:  DocumentSchema.ColumnsNames,
		ColumnValues: []interface{}{id, doc},
		TableSchema:  DocumentSchema.Columns,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{"_id"},
			KeyValues: []interface{}{id},
		},
	}
}

func deleteItem(id string) abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:        abstract.DeleteKind,
		TableSchema: DocumentSchema.Columns,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{"_id"},
			KeyValues: []interface{}{id},
		},
	}
}

// The taker of the unique value comes before the freer: round one defers it, round two applies.
func TestRetryCollectionOrdered_UniqueValueMove(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := retryTestSinker(t, ctx, "retry_unique_move")

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 1}}),
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}}),
	}
	require.NoError(t, s.retryCollectionOrdered(ctx, namespace, items, false))

	var a, b bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "a"}).Decode(&a))
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "b"}).Decode(&b))
	_, aHasU := a["u"]
	require.False(t, aHasU)
	require.EqualValues(t, 1, b["u"])
}

// Ops of one document must not overtake its earlier deferred op: insert x is deferred by the unique
// conflict, so delete x waits with it instead of being applied first (and resurrecting x on retry).
func TestRetryCollectionOrdered_SameIDKeepsOrder(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := retryTestSinker(t, ctx, "retry_same_id")

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "c"}, {Key: "u", Value: 1}})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("x", bson.D{{Key: "_id", Value: "x"}, {Key: "u", Value: 1}}),
		deleteItem("x"),
		upsertItem("c", bson.D{{Key: "_id", Value: "c"}}),
	}
	require.NoError(t, s.retryCollectionOrdered(ctx, namespace, items, false))

	require.ErrorIs(t, coll.FindOne(ctx, bson.M{"_id": "x"}).Err(), mongo_driver.ErrNoDocuments)
	var c bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "c"}).Decode(&c))
	_, cHasU := c["u"]
	require.False(t, cHasU)
}

// The retry prepares the writes a window at a time: the write freeing the unique value is in a later window than its
// taker, and a later write of the deferred document waits for it in its own window.
func TestRetryOrdered_FreeingWriteInLaterWindow(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := retryTestSinker(t, ctx, "retry_windows")
	oneByOne := func(pending []abstract.ChangeItem) ([][]abstract.ChangeItem, error) {
		windows := make([][]abstract.ChangeItem, 0, len(pending))
		for i := range pending {
			windows = append(windows, pending[i:i+1])
		}
		return windows, nil
	}

	_, err := coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "u", Value: 3}},
	})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 3}}),
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 3}, {Key: "v", Value: 2}}),
		upsertItem("c", bson.D{{Key: "_id", Value: "c"}, {Key: "u", Value: 4}}),
	}
	require.NoError(t, s.retryOrdered(ctx, namespace, items, false, oneByOne))

	var a, c bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "a"}).Decode(&a))
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "c"}).Decode(&c))
	require.Equal(t, bson.M{"_id": "a", "u": int32(3), "v": int32(2)}, a)
	require.Equal(t, bson.M{"_id": "c", "u": int32(4)}, c)

	// without the freeing write a round over all the windows applies nothing
	err = s.retryOrdered(ctx, namespace, []abstract.ChangeItem{
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 4}}),
		upsertItem("d", bson.D{{Key: "_id", Value: "d"}, {Key: "u", Value: 3}}),
	}, false, oneByOne)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot make progress")
}

func TestRetryCollectionOrdered_PartialPatchProgress(t *testing.T) {
	testRetryPartialPatchProgress(t, false)
}

func TestRetryOrdered_PartialPatchSameIDAcrossWindows(t *testing.T) {
	testRetryPartialPatchProgress(t, true)
}

func testRetryPartialPatchProgress(t *testing.T, separateWindows bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, target, source, namespace := patchTestSinker(t, ctx, "retry_partial_patch_progress")
	_, err := target.Indexes().CreateOne(ctx, mongo_driver.IndexModel{
		Keys:    bson.D{{Key: "arr.u", Value: 1}},
		Options: mongo_options.Index().SetUnique(true),
	})
	require.NoError(t, err)
	keep := strings.Repeat("x", 1024)
	_, err = target.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "arr", Value: bson.A{
			bson.D{{Key: "u", Value: 1}, {Key: "keep", Value: keep}}, bson.D{{Key: "u", Value: 4}},
		}}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "arr", Value: bson.A{
			bson.D{{Key: "u", Value: 3}, {Key: "keep", Value: keep}}, bson.D{{Key: "u", Value: 2}},
		}}},
	})
	require.NoError(t, err)
	_, err = source.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "arr", Value: bson.A{
			bson.D{{Key: "u", Value: 2}, {Key: "v", Value: 2}, {Key: "keep", Value: keep}},
		}}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "arr", Value: bson.A{
			bson.D{{Key: "u", Value: 1}, {Key: "keep", Value: keep}},
		}}},
	})
	require.NoError(t, err)

	// Each truncation succeeds, but both following sets and their refetches conflict in the first round.
	items := inCollection(namespace,
		patchItem("a", bson.D{{Key: "arr.0.u", Value: 2}, {Key: "arr.0.v", Value: 1}}, nil,
			[]TruncatedArray{{Field: "arr", NewSize: 1}}),
		patchItem("a", bson.D{{Key: "arr.0.v", Value: 2}}, nil, nil),
		patchItem("b", bson.D{{Key: "arr.0.u", Value: 1}}, nil, []TruncatedArray{{Field: "arr", NewSize: 1}}),
	)
	if separateWindows {
		err = s.retryOrdered(ctx, namespace, items, false, func(
			pending []abstract.ChangeItem,
		) ([][]abstract.ChangeItem, error) {
			windows := make([][]abstract.ChangeItem, 0, len(pending))
			for i := range pending {
				windows = append(windows, pending[i:i+1])
			}
			return windows, nil
		})
	} else {
		err = s.retryCollectionOrdered(ctx, namespace, items, false)
	}
	require.NoError(t, err)
	require.GreaterOrEqual(t, s.source.(*collectionSource).finds.Load(), int64(2))
	require.Equal(t, documentsByID(t, ctx, source), documentsByID(t, ctx, target))
}

func TestRetryCollectionOrdered_PartialPatchNoProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, target, source, namespace := patchTestSinker(t, ctx, "retry_partial_patch_no_progress")
	_, err := target.Indexes().CreateOne(ctx, mongo_driver.IndexModel{
		Keys:    bson.D{{Key: "arr.u", Value: 1}},
		Options: mongo_options.Index().SetUnique(true),
	})
	require.NoError(t, err)
	_, err = target.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "arr", Value: bson.A{
			bson.D{{Key: "u", Value: 1}}, bson.D{{Key: "u", Value: 4}},
		}}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "arr", Value: bson.A{bson.D{{Key: "u", Value: 2}}}}},
	})
	require.NoError(t, err)
	_, err = source.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "a"}, {Key: "arr", Value: bson.A{bson.D{{Key: "u", Value: 2}}}},
	})
	require.NoError(t, err)

	// Repeating the successful truncation is a no-op and must not keep a permanent conflict retrying.
	err = s.retryCollectionOrdered(ctx, namespace, inCollection(namespace,
		patchItem("a", bson.D{{Key: "arr.0.u", Value: 2}}, nil, []TruncatedArray{{Field: "arr", NewSize: 1}}),
	), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot make progress")
	require.NoError(t, ctx.Err())
	require.Equal(t, bson.A{bson.M{"u": int32(1)}}, documentsByID(t, ctx, target)["a"]["arr"])
}

// A swap of unique values between two documents cannot be applied by single writes in any order.
func TestRetryCollectionOrdered_NoProgress(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := retryTestSinker(t, ctx, "retry_no_progress")

	_, err := coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 2}},
	})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 2}}),
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 1}}),
	}
	err = s.retryCollectionOrdered(ctx, namespace, items, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot make progress")
}

func preparedModels(t *testing.T, ctx context.Context, s *sinker, namespace Namespace, items []abstract.ChangeItem) []mongo_driver.WriteModel {
	writes, err := s.itemsToPreparedWrites(ctx, namespace, items, false)
	require.NoError(t, err)
	models := make([]mongo_driver.WriteModel, 0, len(writes))
	for _, write := range writes {
		models = append(models, write.model)
	}
	return models
}

// An insert event is a plain insert; delivered twice (restart replay) it turns into a replace of the same document.
func TestBulkWrite_ReinsertReplacesDocument(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := retryTestSinker(t, ctx, "reinsert_unordered")

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: "old"}})
	require.NoError(t, err)

	// every document carries its own "u": the unique index of the fixture is not sparse, two docs without it collide
	models := preparedModels(t, ctx, s, namespace, []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 5}, {Key: "w", Value: "new"}}),
		upsertItem("n", bson.D{{Key: "_id", Value: "n"}, {Key: "u", Value: 6}, {Key: "w", Value: "x"}}),
	})
	require.IsType(t, &mongo_driver.InsertOneModel{}, models[0])
	require.NoError(t, s.bulkWrite(ctx, namespace, models))

	var a, n bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "a"}).Decode(&a))
	require.EqualValues(t, 5, a["u"])
	require.Equal(t, "new", a["w"])
	_, aHasV := a["v"]
	require.False(t, aHasV, "replace-upsert fallback must replace the whole document")
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "n"}).Decode(&n))
	require.Equal(t, "x", n["w"])
}

// With OrderedWrites the bulk stops at the duplicate: the fallback must resume from it, not drop the rest, and a
// second duplicate further down the tail (b) must not need a round of its own.
func TestBulkWrite_OrderedReinsertContinues(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := retryTestSinker(t, ctx, "reinsert_ordered")
	s.config.OrderedWrites = true

	_, err := coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: "old"}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 3}, {Key: "v", Value: "old"}},
	})
	require.NoError(t, err)

	models := preparedModels(t, ctx, s, namespace, []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: "new"}}),
		upsertItem("n", bson.D{{Key: "_id", Value: "n"}, {Key: "u", Value: 2}, {Key: "v", Value: "x"}}),
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: "newer"}}),
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 3}, {Key: "v", Value: "new"}}),
	})
	require.NoError(t, s.bulkWrite(ctx, namespace, models))

	var a, b, n bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "a"}).Decode(&a))
	require.Equal(t, "newer", a["v"])
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "b"}).Decode(&b))
	require.Equal(t, "new", b["v"])
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "n"}).Decode(&n))
	require.Equal(t, "x", n["v"])
}

// Repeated documents of a push fold into one write each (a hot document must not crumble the push into bulks of a
// few documents), and every document ends up in the state of its last event.
func TestPush_RepeatedDocumentsFold(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := testSinker(t, ctx, "push_repeated_docs", false)

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "d"}, {Key: "u", Value: 10}})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: 1}}),
		replaceItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: 2}}),
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 2}}),
		deleteItem("b"),
		upsertItem("c", bson.D{{Key: "_id", Value: "c"}, {Key: "u", Value: 3}}),
		upsertItem("c", bson.D{{Key: "_id", Value: "c"}, {Key: "u", Value: 4}}), // an insert delivered twice
		deleteItem("d"),
		upsertItem("d", bson.D{{Key: "_id", Value: "d"}, {Key: "u", Value: 5}}), // re-created after a delete
		replaceItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: 3}}),
	}
	for i := range items {
		items[i].Schema, items[i].Table = namespace.Database, namespace.Collection
	}

	shardBulks, _, err := s.splitItemsToBulkOperations(ctx, namespace, items, false)
	require.NoError(t, err)
	writes := 0
	for _, bulks := range shardBulks {
		require.Len(t, bulks, 1, "nothing to cut: one bulk per shard")
		writes += len(bulks[0])
	}
	require.Equal(t, 4, writes, "one write per document")

	require.NoError(t, s.Push(items))

	var a, c, d bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "a"}).Decode(&a))
	require.EqualValues(t, 3, a["v"])
	require.ErrorIs(t, coll.FindOne(ctx, bson.M{"_id": "b"}).Err(), mongo_driver.ErrNoDocuments)
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "c"}).Decode(&c))
	require.EqualValues(t, 4, c["u"])
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "d"}).Decode(&d))
	require.EqualValues(t, 5, d["u"])
}

// sameShardIDs returns n document ids that the sink routes to one write shard, so that their bulks run one after
// another.
func sameShardIDs(n int) []string {
	ids := []string{}
	for i := 0; len(ids) < n; i++ {
		id := fmt.Sprintf("doc%d", i)
		if shardOfDocumentID(fmt.Sprintf("%#v", id), sinkWriteConcurrency) == 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

// A unique secondary index turns fold off. Folded, the bulk would be [a: u=4, b: u=1, d: u=5, c: u=3]: the first two
// fail on the index, the last two pass, and the ordered retry of the source events never converges, since a: u=3 now
// collides with c. Unfolded, the bulks follow the source order and the retry can apply the freers before the takers.
func TestPush_UniqueIndexDisablesFold(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := retryTestSinker(t, ctx, "push_unique_index")
	ids := sameShardIDs(4)
	a, b, c, d := ids[0], ids[1], ids[2], ids[3]
	_, err := coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: a}, {Key: "u", Value: 1}},
		bson.D{{Key: "_id", Value: b}, {Key: "u", Value: 2}},
		bson.D{{Key: "_id", Value: d}, {Key: "u", Value: 4}},
		bson.D{{Key: "_id", Value: c}, {Key: "u", Value: 6}},
	})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		replaceItem(a, bson.D{{Key: "_id", Value: a}, {Key: "u", Value: 3}}),
		replaceItem(b, bson.D{{Key: "_id", Value: b}, {Key: "u", Value: 1}}),
		replaceItem(d, bson.D{{Key: "_id", Value: d}, {Key: "u", Value: 5}}),
		replaceItem(a, bson.D{{Key: "_id", Value: a}, {Key: "u", Value: 4}}),
		replaceItem(c, bson.D{{Key: "_id", Value: c}, {Key: "u", Value: 3}}),
	}
	for i := range items {
		items[i].Schema, items[i].Table = namespace.Database, namespace.Collection
	}

	shardBulks, _, err := s.splitItemsToBulkOperations(ctx, namespace, items, false)
	require.NoError(t, err)
	require.Len(t, shardBulks, 1)
	require.Len(t, shardBulks[0], 2, "the repeated document cuts the bulk")
	require.Len(t, shardBulks[0][0], 3)
	require.Len(t, shardBulks[0][1], 2)

	require.NoError(t, s.Push(items))
	for id, u := range map[string]int{a: 4, b: 1, d: 5, c: 3} {
		var doc bson.M
		require.NoError(t, coll.FindOne(ctx, bson.M{"_id": id}).Decode(&doc))
		require.EqualValues(t, u, doc["u"], id)
	}
}

func TestUniqueSecondaryIndex(t *testing.T) {
	id := indexSpec{Key: bson.D{{Key: "_id", Value: 1}}, Unique: false, PrepareUnique: false}
	u := bson.D{{Key: "u", Value: 1}}
	require.False(t, uniqueSecondaryIndex([]indexSpec{id}))
	require.False(t, uniqueSecondaryIndex([]indexSpec{id, {Key: bson.D{{Key: "_id", Value: 1}}, Unique: true}}))
	require.False(t, uniqueSecondaryIndex([]indexSpec{id, {Key: u}}))
	require.True(t, uniqueSecondaryIndex([]indexSpec{id, {Key: u, Unique: true}}))
	require.True(t, uniqueSecondaryIndex([]indexSpec{id, {Key: u, PrepareUnique: true}}))
	require.True(t, uniqueSecondaryIndex([]indexSpec{
		id, {Key: bson.D{{Key: "_id", Value: 1}, {Key: "u", Value: 1}}, Unique: true},
	}))
}

// The driver returns raw counters for an ordered bulk that stopped on an error: the upsert of a new document is
// counted as matched too, and the fallback must not sum it in.
func TestBulkWrite_OrderedFallbackCounters(t *testing.T) {
	ctx := context.Background()
	s, coll, namespace := testSinker(t, ctx, "ordered_fallback_counters", false)
	s.config.OrderedWrites = true
	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "b"}, {Key: "v", Value: "old"}})
	require.NoError(t, err)

	models := preparedModels(t, ctx, s, namespace, []abstract.ChangeItem{
		replaceItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: "new"}}),
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "v", Value: "new"}}),
	})
	result, err := s.bulkWriteWithFallbacks(ctx, namespace, coll, models, mongo_options.BulkWrite().SetOrdered(true))
	require.NoError(t, err)
	require.EqualValues(t, 1, result.UpsertedCount)
	require.EqualValues(t, 1, result.MatchedCount)
}

// A heterogeneous insert (a row without _id) becomes a plain insert with the generated _id at the root of the document.
func TestMakeWriteModel_HeterogeneousInsert(t *testing.T) {
	item := abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "v"},
		ColumnValues: []interface{}{int64(7), "x"},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: "int64", PrimaryKey: true},
			{ColumnName: "v", DataType: "utf8"},
		}),
	}
	id, err := getDocumentID(&item)
	require.NoError(t, err)
	require.NotNil(t, id.Raw)

	models, err := (&sinker{}).makeWriteModels(&item, id, nil, &shardedCollectionSinkContext{})
	require.NoError(t, err)
	require.Len(t, models, 1)
	insert, ok := models[0].(*mongo_driver.InsertOneModel)
	require.True(t, ok)
	require.Equal(t, bson.D{{Key: "_id", Value: id.Raw}, {Key: "id", Value: int64(7)}, {Key: "v", Value: "x"}}, insert.Document)
}

// Exercise nontrivial sharded preparation against the ordinary CI recipe, then apply its writes to real MongoDB.
func TestPrepareShardedPatches_UniqueValueCycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, target, source, namespace := patchTestSinker(t, ctx, "patch_prepare_unique_cycle")
	key, err := GetShardingKey(ctx, s.client, namespace)
	require.NoError(t, err)
	require.Nil(t, key, "this lower-level test supplies its own explicit sharding context")
	items := uniqueCycleEvents(t, ctx, target, source, namespace, 1)
	expected := documentsByID(t, ctx, source)
	for attempt := 0; attempt < 2; attempt++ {
		sharding := &shardedCollectionSinkContext{
			client:       s.client,
			collection:   namespace,
			shardKey:     &ShardKeysInfo{Key: bson.D{{Key: "tenant", Value: 1}}},
			docKeysCache: map[string]bson.M{},
			enabled:      true,
		}
		require.True(t, sharding.Enabled())
		require.False(t, sharding.IsTrivial())
		readsBefore := s.source.(*collectionSource).finds.Load()
		writes, err := s.itemsToPreparedWritesWithSharding(ctx, namespace, items, sharding)
		require.NoError(t, err)
		require.Equal(t, readsBefore, s.source.(*collectionSource).finds.Load(), "preparation must not read the source")
		require.Len(t, writes, 3)
		for i, write := range writes {
			require.Equal(t, i, write.item, "every source event retains its position")
			_, err := s.bulkWriteWithFallbacks(ctx, namespace, target,
				[]mongo_driver.WriteModel{write.model}, mongo_options.BulkWrite())
			require.NoError(t, err, "attempt %d, event %d must retain the intermediate unique-value release", attempt, i)
		}
		require.Equal(t, expected, documentsByID(t, ctx, target), "attempt %d", attempt)
	}
}

func TestPush_ShardedPatchReplayWithTakenValue(t *testing.T) {
	for _, tenant := range []int32{1, 99} {
		t.Run(fmt.Sprintf("tenant=%d", tenant), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s, target, source, namespace := shardedPatchTestSinker(t, ctx, fmt.Sprintf("patch_taken_value_%d", tenant))
			items := uniqueCycleEvents(t, ctx, target, source, namespace, tenant)
			for _, item := range items {
				_, err := target.UpdateOne(ctx,
					bson.D{{Key: "_id", Value: getNativeDocumentID(&item)}, {Key: "tenant", Value: tenant}},
					bson.D{{Key: "$set", Value: item.ColumnValues[1]}},
				)
				require.NoError(t, err)
			}
			for _, collection := range []*mongo_driver.Collection{target, source} {
				_, err := collection.InsertOne(ctx,
					bson.D{{Key: "_id", Value: "c"}, {Key: "tenant", Value: tenant}, {Key: "u", Value: 2}})
				require.NoError(t, err)
			}
			expected := documentsByID(t, ctx, source)
			require.NoError(t, s.Push(items))
			require.Equal(t, expected, documentsByID(t, ctx, target))
			require.NotZero(t, s.source.(*collectionSource).finds.Load(), "replay must recover the taken value")
		})
	}
}

// The divergent target forces mismatch recovery to move a document before the following routed full write.
func TestPush_ShardedPatchRefetchThenFullWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, target, source, namespace := shardedPatchTestSinker(t, ctx, "patch_refetch_then_full")
	before := bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: 1}, {Key: "n", Value: 5}}
	current := bson.D{
		{Key: "_id", Value: "a"}, {Key: "tenant", Value: 99},
		{Key: "n", Value: bson.D{{Key: "x", Value: 7}}}, {Key: "phase", Value: "done"},
	}
	_, err := target.InsertOne(ctx, before)
	require.NoError(t, err)
	_, err = source.InsertOne(ctx, current)
	require.NoError(t, err)

	require.NoError(t, s.Push(inCollection(namespace,
		replaceItem("a", before),
		patchItem("a", bson.D{{Key: "n.x", Value: 1}}, nil, nil),
		replaceItem("a", current),
	)))
	require.NotZero(t, s.source.(*collectionSource).finds.Load(), "the mismatch must take the refetch path")
	require.Equal(t, documentsByID(t, ctx, source), documentsByID(t, ctx, target))
	count, err := target.CountDocuments(ctx, bson.D{{Key: "_id", Value: "a"}})
	require.NoError(t, err)
	require.EqualValues(t, 1, count, "routing changes must not create duplicate _id values across shards")
}

func TestPush_ShardedPatchOfMissingDocument(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered=%v", ordered), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s, target, source, namespace := shardedPatchTestSinker(t, ctx, fmt.Sprintf("patch_missing_sharded_%v", ordered))
			s.config.OrderedWrites = ordered
			_, err := source.InsertOne(ctx, bson.D{
				{Key: "_id", Value: "a"}, {Key: "tenant", Value: 99},
				{Key: "v", Value: 2}, {Key: "keep", Value: "complete source document"},
			})
			require.NoError(t, err)

			require.NoError(t, s.Push(inCollection(namespace, patchItem("a", bson.D{{Key: "v", Value: 1}}, nil, nil))))
			require.NotZero(t, s.source.(*collectionSource).finds.Load(), "the missing target must be healed")
			require.Equal(t, documentsByID(t, ctx, source), documentsByID(t, ctx, target))
			count, err := target.CountDocuments(ctx, bson.D{{Key: "_id", Value: "a"}})
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
		})
	}
}

func TestIsRoutingSafePatch(t *testing.T) {
	cases := []struct {
		name, path string
		keys       []string
		safe       bool
	}{
		{"same", "tenant", []string{"tenant"}, false},
		{"ancestor", "tenant", []string{"tenant.id"}, false},
		{"descendant", "tenant.id", []string{"tenant"}, false},
		{"array descendant", "tenant.0", []string{"tenant"}, false},
		{"unrelated", "u", []string{"tenant"}, true},
		{"text prefix", "tenantExtra", []string{"tenant"}, true},
		{"nested text prefix", "tenant.identifier", []string{"tenant.id"}, true},
		{"second key", "region", []string{"tenant", "region"}, false},
		{"second key ancestor", "region", []string{"tenant", "region.id"}, false},
		{"multiple unrelated", "u", []string{"tenant", "region.id"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set := patchItem("a", bson.D{{Key: tc.path, Value: 1}}, nil, nil)
			unset := patchItem("a", nil, []string{tc.path}, nil)
			truncate := patchItem("a", nil, nil, []TruncatedArray{{Field: tc.path, NewSize: 1}})
			require.Equal(t, tc.safe, isRoutingSafePatch(&set, tc.keys), "updated path")
			require.Equal(t, tc.safe, isRoutingSafePatch(&unset, tc.keys), "removed path")
			require.Equal(t, tc.safe, isRoutingSafePatch(&truncate, tc.keys), "truncated path")
		})
	}
	empty := patchItem("a", nil, nil, nil)
	require.True(t, isRoutingSafePatch(&empty, []string{"tenant"}))
	full := replaceItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: 1}})
	require.False(t, isRoutingSafePatch(&full, []string{"tenant"}))
	invalid := patchItem("a", nil, nil, nil)
	invalid.ColumnValues[1] = "invalid updated fields"
	require.False(t, isRoutingSafePatch(&invalid, []string{"tenant"}))
}

func TestPrepareShardedPatches_MultipleModelsUseIdentity(t *testing.T) {
	ctx := context.Background()
	s, target, _, namespace := patchTestSinker(t, ctx, "patch_prepare_multiple_models")
	_, err := target.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "a"}, {Key: "tenant", Value: 1}, {Key: "arr", Value: bson.A{0, 1, 2}},
	})
	require.NoError(t, err)
	sharding := &shardedCollectionSinkContext{
		client:       s.client,
		collection:   namespace,
		shardKey:     &ShardKeysInfo{Key: bson.D{{Key: "tenant", Value: 1}}},
		docKeysCache: map[string]bson.M{},
		enabled:      true,
	}
	items := inCollection(namespace, patchItem("a", bson.D{{Key: "arr.1", Value: 8}}, nil,
		[]TruncatedArray{{Field: "arr", NewSize: 2}}))
	writes, err := s.itemsToPreparedWritesWithSharding(ctx, namespace, items, sharding)
	require.NoError(t, err)
	require.Zero(t, s.source.(*collectionSource).finds.Load())
	require.Len(t, writes, 2)
	for _, write := range writes {
		model, ok := write.model.(*mongo_driver.UpdateOneModel)
		require.True(t, ok)
		require.Equal(t, bson.D{{Key: "_id", Value: "a"}}, model.Filter)
		require.True(t, model.Upsert == nil || !*model.Upsert, "patches must not upsert partial documents")
		require.Zero(t, write.item, "both models belong to the same event")
		_, err := s.bulkWriteWithFallbacks(ctx, namespace, target,
			[]mongo_driver.WriteModel{model}, mongo_options.BulkWrite())
		require.NoError(t, err)
	}
	require.Equal(t, map[string]bson.M{
		"a": {"_id": "a", "tenant": int32(1), "arr": bson.A{int32(0), int32(8)}},
	}, documentsByID(t, ctx, target))
}

// Both rejected patches share one bulk; their refetched documents must cross a real shard boundary.
func TestPush_ShardedPatchRefetchMultipleRelocations(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered=%v", ordered), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s, target, source, namespace := shardedPatchTestSinker(t, ctx,
				fmt.Sprintf("patch_refetch_multiple_moves_%v", ordered))
			s.config.OrderedWrites = ordered
			ids := sameShardIDs(2)
			require.Equal(t, shardOfDocumentID(documentIDString(ids[0]), sinkWriteConcurrency),
				shardOfDocumentID(documentIDString(ids[1]), sinkWriteConcurrency))
			var items []abstract.ChangeItem
			for _, id := range ids {
				_, err := target.InsertOne(ctx,
					bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: 1}, {Key: "n", Value: 5}})
				require.NoError(t, err)
				_, err = source.InsertOne(ctx, bson.D{
					{Key: "_id", Value: id}, {Key: "tenant", Value: 99},
					{Key: "n", Value: bson.D{{Key: "x", Value: 7}}},
				})
				require.NoError(t, err)
				items = append(items, patchItem(id, bson.D{{Key: "n.x", Value: 1}}, nil, nil))
			}
			items = inCollection(namespace, items...)
			bulks, folded, err := s.splitItemsToBulkOperations(ctx, namespace, items, false)
			require.NoError(t, err)
			require.Zero(t, folded)
			require.Len(t, bulks, 1)
			require.Len(t, bulks[0], 1)
			require.Len(t, bulks[0][0], 2, "both patches must reach the same normal Push bulk")
			require.Zero(t, s.source.(*collectionSource).finds.Load(), "preparation must retain both patches")

			_, mismatchErr := target.BulkWrite(ctx, bulks[0][0], mongo_options.BulkWrite().SetOrdered(false))
			var mismatch mongo_driver.BulkWriteException
			require.ErrorAs(t, mismatchErr, &mismatch)
			require.Len(t, mismatch.WriteErrors, 2, "both initial target documents must reject the patch")
			for _, failed := range mismatch.WriteErrors {
				require.True(t, s.canRefetch(failed), "the preflight error must enter mismatch recovery")
			}
			pushErr := s.Push(items)
			t.Logf("Push with two tenant relocations: %v; target: %v", pushErr, documentsByID(t, ctx, target))
			require.NoError(t, pushErr)
			require.NotZero(t, s.source.(*collectionSource).finds.Load(), "the two rejected patches must refetch")
			require.Equal(t, documentsByID(t, ctx, source), documentsByID(t, ctx, target))
			for _, id := range ids {
				count, err := target.CountDocuments(ctx, bson.D{{Key: "_id", Value: id}})
				require.NoError(t, err)
				require.EqualValues(t, 1, count, "relocation must keep exactly one target document for %s", id)
			}
		})
	}
}

func TestRetryOrdered_WindowSelectionFailure(t *testing.T) {
	ctx := context.Background()
	s, target, source, namespace := patchTestSinker(t, ctx, "retry_window_selection_failure")
	_, err := target.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: 1}})
	require.NoError(t, err)
	_, err = source.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: 3}})
	require.NoError(t, err)
	before := documentsByID(t, ctx, target)
	items := inCollection(namespace,
		patchItem("a", bson.D{{Key: "v", Value: 2}}, nil, nil),
		replaceItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: 3}}),
	)
	injected := errors.New("injected window discovery failure")
	calls := 0
	err = s.retryOrdered(ctx, namespace, items, false, func(
		pending []abstract.ChangeItem,
	) ([][]abstract.ChangeItem, error) {
		calls++
		require.Equal(t, items, pending)
		return nil, injected
	})
	require.ErrorIs(t, err, injected)
	require.Equal(t, 1, calls)
	require.Equal(t, before, documentsByID(t, ctx, target), "failed discovery must not apply the pending writes")
	require.Zero(t, s.source.(*collectionSource).finds.Load())
	require.NoError(t, s.retryCollectionOrdered(ctx, namespace, items, false))
	require.Equal(t, documentsByID(t, ctx, source), documentsByID(t, ctx, target))
}
