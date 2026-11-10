package mongo

import (
	"context"
	"fmt"
	"testing"

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

	collID := Namespace{Database: "test_db", Collection: collection}
	coll := client.Database(collID.Database).Collection(collID.Collection)
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
	}, coll, collID
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
	s, coll, collID := retryTestSinker(t, ctx, "retry_unique_move")

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 1}}),
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}}),
	}
	require.NoError(t, s.retryCollectionOrdered(ctx, collID, items))

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
	s, coll, collID := retryTestSinker(t, ctx, "retry_same_id")

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "c"}, {Key: "u", Value: 1}})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("x", bson.D{{Key: "_id", Value: "x"}, {Key: "u", Value: 1}}),
		deleteItem("x"),
		upsertItem("c", bson.D{{Key: "_id", Value: "c"}}),
	}
	require.NoError(t, s.retryCollectionOrdered(ctx, collID, items))

	require.ErrorIs(t, coll.FindOne(ctx, bson.M{"_id": "x"}).Err(), mongo_driver.ErrNoDocuments)
	var c bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "c"}).Decode(&c))
	_, cHasU := c["u"]
	require.False(t, cHasU)
}

// A swap of unique values between two documents cannot be applied by single writes in any order.
func TestRetryCollectionOrdered_NoProgress(t *testing.T) {
	ctx := context.Background()
	s, coll, collID := retryTestSinker(t, ctx, "retry_no_progress")

	_, err := coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 2}},
	})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 2}}),
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 1}}),
	}
	err = s.retryCollectionOrdered(ctx, collID, items)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot make progress")
}

func preparedModels(t *testing.T, ctx context.Context, s *sinker, collID Namespace, items []abstract.ChangeItem) []mongo_driver.WriteModel {
	writes, err := s.itemsToPreparedWrites(ctx, collID, items)
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
	s, coll, collID := retryTestSinker(t, ctx, "reinsert_unordered")

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: "old"}})
	require.NoError(t, err)

	// every document carries its own "u": the unique index of the fixture is not sparse, two docs without it collide
	models := preparedModels(t, ctx, s, collID, []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 5}, {Key: "w", Value: "new"}}),
		upsertItem("n", bson.D{{Key: "_id", Value: "n"}, {Key: "u", Value: 6}, {Key: "w", Value: "x"}}),
	})
	require.IsType(t, &mongo_driver.InsertOneModel{}, models[0])
	require.NoError(t, s.bulkWrite(ctx, collID, models))

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
	s, coll, collID := retryTestSinker(t, ctx, "reinsert_ordered")
	s.config.OrderedWrites = true

	_, err := coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: "old"}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 3}, {Key: "v", Value: "old"}},
	})
	require.NoError(t, err)

	models := preparedModels(t, ctx, s, collID, []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: "new"}}),
		upsertItem("n", bson.D{{Key: "_id", Value: "n"}, {Key: "u", Value: 2}, {Key: "v", Value: "x"}}),
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}, {Key: "v", Value: "newer"}}),
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 3}, {Key: "v", Value: "new"}}),
	})
	require.NoError(t, s.bulkWrite(ctx, collID, models))

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
	s, coll, collID := testSinker(t, ctx, "push_repeated_docs", false)

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
		items[i].Schema, items[i].Table = collID.Database, collID.Collection
	}

	shardBulks, err := s.splitItemsToBulkOperations(ctx, collID, items)
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
	s, coll, collID := retryTestSinker(t, ctx, "push_unique_index")
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
		items[i].Schema, items[i].Table = collID.Database, collID.Collection
	}

	shardBulks, err := s.splitItemsToBulkOperations(ctx, collID, items)
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
	s, coll, collID := testSinker(t, ctx, "ordered_fallback_counters", false)
	s.config.OrderedWrites = true
	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "b"}, {Key: "v", Value: "old"}})
	require.NoError(t, err)

	models := preparedModels(t, ctx, s, collID, []abstract.ChangeItem{
		replaceItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: "new"}}),
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "v", Value: "new"}}),
	})
	result, err := s.bulkWriteWithInsertFallback(ctx, collID, coll, models, mongo_options.BulkWrite().SetOrdered(true))
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

	model, err := (&sinker{}).makeWriteModel(&item, id, nil, &shardedCollectionSinkContext{})
	require.NoError(t, err)
	insert, ok := model.(*mongo_driver.InsertOneModel)
	require.True(t, ok)
	require.Equal(t, bson.D{{Key: "_id", Value: id.Raw}, {Key: "id", Value: int64(7)}, {Key: "v", Value: "x"}}, insert.Document)
}
