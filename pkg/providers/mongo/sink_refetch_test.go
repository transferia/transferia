package mongo

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
)

// collectionSource serves the documents of one collection as the source documents of any target collection.
type collectionSource struct {
	coll  *mongo_driver.Collection
	finds atomic.Int64
}

func (s *collectionSource) Find(
	ctx context.Context, _ Namespace, ids []interface{}, _ primitive.Timestamp,
) (map[string]bson.D, error) {
	s.finds.Add(1)
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return nil, err
	}
	var documents []bson.D
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, err
	}
	result := map[string]bson.D{}
	for _, document := range documents {
		result[documentIDString(document[0].Value)] = document
	}
	return result, nil
}

func (s *collectionSource) Close(context.Context) error {
	return nil
}

// patchTestSinker makes a sinker over the recipe mongo writing to a dropped collection, with another dropped
// collection of the same mongo as the source of its documents.
func patchTestSinker(t *testing.T, ctx context.Context, name string) (*sinker, *mongo_driver.Collection, *mongo_driver.Collection, Namespace) {
	s, target, namespace := testSinker(t, ctx, name, false)
	source := s.client.Database(namespace.Database).Collection(name + "_source")
	require.NoError(t, source.Drop(ctx))
	t.Cleanup(func() { _ = source.Drop(context.Background()) })
	s.source = &collectionSource{coll: source, finds: atomic.Int64{}}
	return s, target, source, namespace
}

func patchItem(id string, set bson.D, unset []string, truncated []TruncatedArray) abstract.ChangeItem {
	if set == nil {
		set = bson.D{}
	}
	if truncated == nil {
		truncated = []TruncatedArray{}
	}
	return abstract.ChangeItem{
		Kind:         abstract.MongoUpdateDocumentKind,
		ColumnNames:  UpdateDocumentSchema.ColumnsNames,
		ColumnValues: []interface{}{id, set, unset, truncated, bson.D{}},
		TableSchema:  UpdateDocumentSchema.Columns,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{"_id"},
			KeyValues: []interface{}{id},
		},
	}
}

func inCollection(namespace Namespace, items ...abstract.ChangeItem) []abstract.ChangeItem {
	for i := range items {
		items[i].Schema, items[i].Table = namespace.Database, namespace.Collection
	}
	return items
}

func documentsByID(t *testing.T, ctx context.Context, coll *mongo_driver.Collection) map[string]bson.M {
	cursor, err := coll.Find(ctx, bson.D{})
	require.NoError(t, err)
	var documents []bson.M
	require.NoError(t, cursor.All(ctx, &documents))
	result := map[string]bson.M{}
	for _, document := range documents {
		result[fmt.Sprint(document["_id"])] = document
	}
	return result
}

func TestPush_PatchChangesDocumentInPlace(t *testing.T) {
	ctx := context.Background()
	s, target, _, namespace := patchTestSinker(t, ctx, "patch_in_place")
	_, err := target.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "a"},
		{Key: "keep", Value: 1},
		{Key: "drop", Value: 1},
		{Key: "n", Value: bson.D{{Key: "x", Value: 1}}},
		{Key: "arr", Value: bson.A{1, 2, 3, 4}},
		{Key: "objs", Value: bson.A{bson.D{{Key: "x", Value: 1}}, bson.D{{Key: "x", Value: 2}}, bson.D{{Key: "x", Value: 3}}}},
		{Key: "grid", Value: bson.A{bson.A{1, 2, 3}, bson.A{4, 5, 6}, bson.A{7}}},
	})
	require.NoError(t, err)

	require.NoError(t, s.Push(inCollection(namespace,
		patchItem("a", bson.D{{Key: "grid.1.0", Value: 40}}, nil,
			[]TruncatedArray{{Field: "grid.0", NewSize: 1}, {Field: "grid", NewSize: 2}}),
		patchItem("a", bson.D{{Key: "n.x", Value: 2}, {Key: "new", Value: "v"}}, []string{"drop"}, nil),
		patchItem("a", nil, nil, []TruncatedArray{{Field: "arr", NewSize: 2}}),
		patchItem("a", bson.D{{Key: "objs.0.x", Value: 10}}, []string{"objs.1.x"}, []TruncatedArray{{Field: "objs", NewSize: 2}}),
		patchItem("a", nil, nil, nil), // nothing changed
	)))

	require.Equal(t, map[string]bson.M{"a": {
		"_id":  "a",
		"keep": int32(1),
		"n":    bson.M{"x": int32(2)},
		"new":  "v",
		"arr":  bson.A{int32(1), int32(2)},
		"objs": bson.A{bson.M{"x": int32(10)}, bson.M{}},
		"grid": bson.A{bson.A{int32(1)}, bson.A{int32(40), int32(5), int32(6)}},
	}}, documentsByID(t, ctx, target))
	require.Zero(t, s.source.(*collectionSource).finds.Load(), "nothing to read from the source")
}

// A patch of a document the target does not have must not create a stub of the changed fields: the document is
// taken from the source, or left absent when the source has none either.
func TestPush_PatchOfMissingDocument(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered=%v", ordered), func(t *testing.T) {
			ctx := context.Background()
			s, target, source, namespace := patchTestSinker(t, ctx, "patch_missing")
			s.config.OrderedWrites = ordered
			_, err := source.InsertOne(ctx, bson.D{{Key: "_id", Value: "lost"}, {Key: "v", Value: 5}, {Key: "w", Value: 1}})
			require.NoError(t, err)
			_, err = target.InsertOne(ctx, bson.D{{Key: "_id", Value: "dup"}, {Key: "v", Value: 0}})
			require.NoError(t, err)

			require.NoError(t, s.Push(inCollection(namespace,
				// the fallback of the re-delivered insert must not hide the patches which matched nothing
				upsertItem("dup", bson.D{{Key: "_id", Value: "dup"}, {Key: "v", Value: 1}}),
				patchItem("lost", bson.D{{Key: "v", Value: 4}}, nil, nil),
				patchItem("deleted", bson.D{{Key: "v", Value: 4}}, nil, nil),
				replaceItem("new", bson.D{{Key: "_id", Value: "new"}, {Key: "v", Value: 1}}),
			)))

			require.Equal(t, map[string]bson.M{
				"dup":  {"_id": "dup", "v": int32(1)},
				"lost": {"_id": "lost", "v": int32(5), "w": int32(1)},
				"new":  {"_id": "new", "v": int32(1)},
			}, documentsByID(t, ctx, target))
		})
	}
}

// The counters of a bulk are kept per kind of write, so an insert does not hide a patch which matched nothing.
func TestBulkWrite_InsertDoesNotHideUnmatchedPatch(t *testing.T) {
	ctx := context.Background()
	s, target, source, namespace := patchTestSinker(t, ctx, "patch_missing_with_insert")
	_, err := source.InsertOne(ctx, bson.D{{Key: "_id", Value: "lost"}, {Key: "v", Value: 5}})
	require.NoError(t, err)

	bulk := append(
		[]mongo_driver.WriteModel{makeInsertModel(bson.D{{Key: "_id", Value: "new"}, {Key: "v", Value: 1}})},
		makeUpdateModels(bson.D{{Key: "_id", Value: "lost"}}, bson.D{{Key: "v", Value: 4}}, nil, nil)...,
	)
	_, err = s.bulkWriteWithFallbacks(ctx, namespace, target, bulk, mongo_options.BulkWrite().SetOrdered(false))
	require.NoError(t, err)

	require.Equal(t, map[string]bson.M{
		"new":  {"_id": "new", "v": int32(1)},
		"lost": {"_id": "lost", "v": int32(5)},
	}, documentsByID(t, ctx, target))
}

func TestSplitByPatchedDocuments(t *testing.T) {
	ids := func(portions [][]abstract.ChangeItem) [][]string {
		result := make([][]string, 0, len(portions))
		for _, portion := range portions {
			portionIDs := []string{}
			for i := range portion {
				portionIDs = append(portionIDs, getNativeDocumentID(&portion[i]).(string))
			}
			result = append(result, portionIDs)
		}
		return result
	}
	patch := func(id string) abstract.ChangeItem { return patchItem(id, bson.D{{Key: "v", Value: 1}}, nil, nil) }
	items := []abstract.ChangeItem{
		upsertItem("n", bson.D{{Key: "_id", Value: "n"}}), patch("a"), patch("a"), patch("b"), patch("a"),
		deleteItem("m"), patch("c"), patch("b"),
	}

	// a repeated patch of a document does not count, and a portion is cut at the patch of a document over the limit
	require.Equal(t, [][]string{{"n", "a", "a", "b", "a", "m"}, {"c", "b"}}, ids(splitByPatchedDocuments(items, 2)))
	require.Equal(t, [][]string{{"n", "a", "a", "b", "a", "m", "c", "b"}}, ids(splitByPatchedDocuments(items, 3)))
	require.Len(t, splitByPatchedDocuments(nil, 2), 1)
}

// A patch made for another state of the document is rejected by the server; the document is then taken from the
// source.
func TestPush_RejectedPatchRefetchesDocument(t *testing.T) {
	rejected := map[string]abstract.ChangeItem{
		"set below a scalar":      patchItem("a", bson.D{{Key: "n.x", Value: 1}}, nil, nil),
		"truncate a non-array":    patchItem("a", nil, nil, []TruncatedArray{{Field: "n", NewSize: 1}}),
		"truncate below a scalar": patchItem("a", nil, nil, []TruncatedArray{{Field: "n.arr", NewSize: 1}}),
	}
	for name, patch := range rejected {
		for _, ordered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, ordered=%v", name, ordered), func(t *testing.T) {
				ctx := context.Background()
				s, target, source, namespace := patchTestSinker(t, ctx, "patch_rejected")
				s.config.OrderedWrites = ordered
				current := bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: bson.D{{Key: "x", Value: 7}}}}
				_, err := source.InsertOne(ctx, current)
				require.NoError(t, err)
				_, err = target.InsertMany(ctx, []interface{}{
					bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: 5}},
					bson.D{{Key: "_id", Value: "b"}, {Key: "v", Value: 1}},
				})
				require.NoError(t, err)

				require.NoError(t, s.Push(inCollection(namespace,
					patchItem("b", bson.D{{Key: "v", Value: 2}}, nil, nil),
					patch,
					patchItem("a", bson.D{{Key: "after", Value: 1}}, nil, nil), // goes over the refetched document
				)))

				require.Equal(t, map[string]bson.M{
					"a": {"_id": "a", "n": bson.M{"x": int32(7)}, "after": int32(1)},
					"b": {"_id": "b", "v": int32(2)},
				}, documentsByID(t, ctx, target))
			})
		}
	}

	t.Run("document is gone from the source", func(t *testing.T) {
		ctx := context.Background()
		s, target, _, namespace := patchTestSinker(t, ctx, "patch_rejected_gone")
		_, err := target.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: 5}})
		require.NoError(t, err)

		require.NoError(t, s.Push(inCollection(namespace, patchItem("a", bson.D{{Key: "n.x", Value: 1}}, nil, nil))))

		// left to the delete event which is to follow
		require.Equal(t, map[string]bson.M{"a": {"_id": "a", "n": int32(5)}}, documentsByID(t, ctx, target))
	})

	t.Run("no document and no access to the source", func(t *testing.T) {
		ctx := context.Background()
		s, _, _, namespace := patchTestSinker(t, ctx, "patch_unmatched_no_source")
		s.source = nil
		err := s.Push(inCollection(namespace, patchItem("a", bson.D{{Key: "v", Value: 1}}, nil, nil)))
		require.ErrorContains(t, err, "found no document")
	})

	t.Run("no access to the source", func(t *testing.T) {
		ctx := context.Background()
		s, target, _, namespace := patchTestSinker(t, ctx, "patch_rejected_no_source")
		s.source = nil
		_, err := target.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: 5}})
		require.NoError(t, err)

		err = s.Push(inCollection(namespace, patchItem("a", bson.D{{Key: "n.x", Value: 1}}, nil, nil)))
		require.ErrorContains(t, err, "Cannot create field")
	})
}

// Delivered again, a patch may bring back a value of a unique index which another document has taken since. The
// ordered retry cannot get past it, the current document of the source can.
func TestPush_PatchWithDuplicateKeyRefetchesDocument(t *testing.T) {
	ctx := context.Background()
	s, target, source, namespace := patchTestSinker(t, ctx, "patch_duplicate")
	_, err := target.Indexes().CreateOne(ctx, mongo_driver.IndexModel{
		Keys:    bson.D{{Key: "u", Value: 1}},
		Options: mongo_options.Index().SetUnique(true),
	})
	require.NoError(t, err)
	current := []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 3}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 1}},
	}
	_, err = source.InsertMany(ctx, current)
	require.NoError(t, err)
	_, err = target.InsertMany(ctx, current)
	require.NoError(t, err)

	// the history of the source: a had u=1, then took 3, then b took 1
	require.NoError(t, s.Push(inCollection(namespace,
		patchItem("a", bson.D{{Key: "u", Value: 1}}, nil, nil),
		patchItem("a", bson.D{{Key: "u", Value: 3}}, nil, nil),
		patchItem("b", bson.D{{Key: "u", Value: 1}}, nil, nil),
	)))

	require.Equal(t, map[string]bson.M{
		"a": {"_id": "a", "u": int32(3)},
		"b": {"_id": "b", "u": int32(1)},
	}, documentsByID(t, ctx, target))
	require.NotZero(t, s.source.(*collectionSource).finds.Load())
}

// Fields of different moments may not fit into a document together.
func TestPush_PatchOverLargerDocumentRefetchesDocument(t *testing.T) {
	ctx := context.Background()
	s, target, source, namespace := patchTestSinker(t, ctx, "patch_too_large")
	large := strings.Repeat("x", 9<<20)
	_, err := source.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "new", Value: large}})
	require.NoError(t, err)
	_, err = target.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "new", Value: large}})
	require.NoError(t, err)

	require.NoError(t, s.Push(inCollection(namespace, patchItem("a", bson.D{{Key: "old", Value: large}}, nil, nil))))

	var document bson.M
	require.NoError(t, target.FindOne(ctx, bson.D{{Key: "_id", Value: "a"}}).Decode(&document))
	require.NotContains(t, document, "old")
	require.EqualValues(t, 1, s.source.(*collectionSource).finds.Load())
}

// Patches touching a shard-key path are resolved into updates carrying the whole document.
func TestResolvePatches(t *testing.T) {
	ctx := context.Background()
	s, _, source, namespace := patchTestSinker(t, ctx, "patch_resolve")
	_, err := source.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: 2}})
	require.NoError(t, err)

	items, origin, err := s.resolvePatches(ctx, namespace, inCollection(namespace,
		upsertItem("c", bson.D{{Key: "_id", Value: "c"}}),
		patchItem("a", bson.D{{Key: "v", Value: 1}}, nil, nil),
		patchItem("gone", bson.D{{Key: "v", Value: 1}}, nil, nil),
		deleteItem("c"),
	), []string{"v"})
	require.NoError(t, err)
	require.Len(t, items, 3)
	require.Equal(t, []int{0, 1, 3}, origin)
	require.Equal(t, abstract.InsertKind, items[0].Kind)
	require.Equal(t, abstract.DeleteKind, items[2].Kind)

	require.Equal(t, abstract.UpdateKind, items[1].Kind)
	id, err := getDocumentID(&items[1])
	require.NoError(t, err)
	require.Equal(t, "a", id.Raw)
	document, err := getDocument(&items[1], nil)
	require.NoError(t, err)
	require.Equal(t, bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: int32(2)}}, document)

	s.source = nil
	_, _, err = s.resolvePatches(ctx, namespace,
		inCollection(namespace, patchItem("a", bson.D{{Key: "v", Value: 1}}, nil, nil)), []string{"v"})
	require.Error(t, err)
}

func TestResolvePatches_RepeatedLongID(t *testing.T) {
	ctx := context.Background()
	s, _, source, namespace := patchTestSinker(t, ctx, "patch_resolve_repeated_long_id")
	id := strings.Repeat("a", 64*1024)
	document := bson.D{{Key: "_id", Value: id}, {Key: "v", Value: int32(300)}}
	_, err := source.InsertOne(ctx, document)
	require.NoError(t, err)

	items := make([]abstract.ChangeItem, 300)
	for i := range items {
		items[i] = patchItem(id, bson.D{{Key: "v", Value: i}}, nil, nil)
		items[i].Counter = i
	}
	items = inCollection(namespace, items...)
	portions := splitByPatchedDocuments(items, refetchChunk)
	require.Len(t, portions, 1)

	resolved, origin, err := s.resolvePatches(ctx, namespace, portions[0], []string{"v"})
	require.NoError(t, err)
	require.Len(t, resolved, len(items))
	require.Len(t, origin, len(items))
	require.EqualValues(t, 1, s.source.(*collectionSource).finds.Load())
	for i := range resolved {
		require.Equal(t, abstract.UpdateKind, resolved[i].Kind)
		require.Equal(t, []interface{}{id, document}, resolved[i].ColumnValues)
		require.Equal(t, i, resolved[i].Counter)
		require.Equal(t, i, origin[i])
	}
}

// replayOps are the updates of the replay test: every kind of change an update description may hold, changes of
// the document structure included. An update the document does not accept fails on the source and makes no event.
func replayOp(random *rand.Rand) interface{} {
	value := random.Intn(1000)
	index := random.Intn(6)
	ifArray := func(field string, then interface{}) bson.D {
		return bson.D{{Key: "$cond", Value: bson.A{bson.D{{Key: "$isArray", Value: "$" + field}}, then, "$" + field}}}
	}
	set := func(field string, v interface{}) bson.D {
		return bson.D{{Key: "$set", Value: bson.D{{Key: field, Value: v}}}}
	}
	unset := func(field string) bson.D {
		return bson.D{{Key: "$unset", Value: bson.D{{Key: field, Value: ""}}}}
	}
	// The structure of a document is broken less often than changed within, to let the arrays live.
	switch choice := random.Intn(100); {
	case choice < 3:
		return set("s", value)
	case choice < 6: // truncates an array of arrays, the outer one and the inner ones
		return mongo_driver.Pipeline{set("grid", ifArray("grid", bson.D{{Key: "$map", Value: bson.D{
			{Key: "input", Value: bson.D{{Key: "$slice", Value: bson.A{"$grid", 3}}}},
			{Key: "as", Value: "row"},
			{Key: "in", Value: bson.D{{Key: "$slice", Value: bson.A{"$$row", index%3 + 1}}}},
		}}}))}
	case choice < 12:
		return set("n.x", value)
	case choice < 15:
		return unset("n.y")
	case choice < 17:
		return set("n", value) // a document turns into a scalar
	case choice < 21:
		return set("n", bson.D{{Key: "x", Value: value}, {Key: "y", Value: value}})
	case choice < 23:
		return unset("n")
	case choice < 33:
		return bson.D{{Key: "$push", Value: bson.D{{Key: "arr", Value: bson.D{{Key: "$each", Value: bson.A{value, value + 1}}}}}}}
	case choice < 37:
		return bson.D{{Key: "$pop", Value: bson.D{{Key: "arr", Value: 1}}}}
	case choice < 47: // truncates the array
		return mongo_driver.Pipeline{set("arr", ifArray("arr", bson.D{{Key: "$slice", Value: bson.A{"$arr", index + 1}}}))}
	case choice < 53:
		return set(fmt.Sprintf("arr.%d", index), value) // pads a shorter array with nulls
	case choice < 55:
		return set("arr", value) // an array turns into a scalar
	case choice < 59:
		return set("arr", bson.A{value, value, value})
	case choice < 60:
		return unset("arr")
	case choice < 72: // truncates the array and changes every element left
		return mongo_driver.Pipeline{
			set("objs", ifArray("objs", bson.D{{Key: "$slice", Value: bson.A{"$objs", index + 1}}})),
			set("objs", ifArray("objs", bson.D{{Key: "$map", Value: bson.D{
				{Key: "input", Value: "$objs"},
				{Key: "as", Value: "o"},
				{Key: "in", Value: bson.D{{Key: "x", Value: value}, {Key: "tag", Value: "$$o.tag"}}},
			}}})),
		}
	case choice < 77:
		return set(fmt.Sprintf("objs.%d.x", index), value)
	case choice < 80:
		return unset(fmt.Sprintf("objs.%d.tag", index))
	case choice < 92:
		return bson.D{{Key: "$push", Value: bson.D{{Key: "objs", Value: bson.D{{Key: "x", Value: value}, {Key: "tag", Value: "t"}}}}}}
	case choice < 94:
		return set("objs", bson.A{bson.D{{Key: "x", Value: value}, {Key: "tag", Value: "t"}}})
	case choice < 96:
		return unset(fmt.Sprintf("arr.%d", index))
	default:
		return set(fmt.Sprintf("n.deep.k%d", index), value)
	}
}

func replayDocument(id string, random *rand.Rand) bson.D {
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "s", Value: random.Intn(1000)},
		{Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "y", Value: 2}}},
		{Key: "arr", Value: bson.A{1, 2, 3, 4, 5}},
		{Key: "grid", Value: bson.A{
			bson.A{"a0", "a1", "a2", "a3"}, bson.A{"b0", "b1", "b2", "b3"},
			bson.A{"c0", "c1", "c2", "c3"}, bson.A{"d0", "d1", "d2", "d3"},
		}},
		{Key: "objs", Value: bson.A{
			bson.D{{Key: "x", Value: 1}, {Key: "tag", Value: "a"}},
			bson.D{{Key: "x", Value: 2}, {Key: "tag", Value: "b"}},
			bson.D{{Key: "x", Value: 3}, {Key: "tag", Value: "c"}},
		}},
	}
}

// replayEvents runs random writes over the source collection and returns their change stream events as the items
// the source of a transfer would make of them.
func replayEvents(t *testing.T, ctx context.Context, source *mongo_driver.Collection, namespace Namespace, random *rand.Rand, ops int) []abstract.ChangeItem {
	stream, err := source.Watch(ctx, mongo_driver.Pipeline{}, mongo_options.ChangeStream().SetFullDocument(mongo_options.Default))
	require.NoError(t, err)
	defer func() { _ = stream.Close(ctx) }()

	byID := bson.D{{Key: "_id", Value: ""}}
	for i := 0; i < ops; i++ {
		id := fmt.Sprintf("doc%d", random.Intn(5))
		byID[0].Value = id
		switch random.Intn(25) {
		case 0:
			_, err = source.DeleteOne(ctx, byID)
			require.NoError(t, err)
		case 1:
			_, err = source.ReplaceOne(ctx, byID, replayDocument(id, random), mongo_options.Replace().SetUpsert(true))
			require.NoError(t, err)
		case 2, 3:
			_, _ = source.InsertOne(ctx, replayDocument(id, random)) // a duplicate is fine
		default:
			_, _ = source.UpdateOne(ctx, byID, replayOp(random))
		}
	}
	const sentinel = "the end"
	_, err = source.InsertOne(ctx, bson.D{{Key: "_id", Value: sentinel}})
	require.NoError(t, err)
	_, err = source.DeleteOne(ctx, bson.D{{Key: "_id", Value: sentinel}})
	require.NoError(t, err)

	converter := &mongoSource{config: &MongoSource{IsHomo: true}}
	var items []abstract.ChangeItem
	readCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		require.True(t, stream.Next(readCtx), "no end of the change stream: %v", stream.Err())
		var event ChangeEvent
		require.NoError(t, stream.Decode(&event))
		if event.DocumentKey.ID == sentinel {
			return inCollection(namespace, items...)
		}
		item, err := converter.makeChangeItem(&puChangeEvent{
			changeEvent:         changeEvent{event: &event, size: len(stream.Current), decodeTime: 0},
			parallelizationUnit: nil,
		})
		require.NoError(t, err)
		items = append(items, item)
	}
}

func pushInBatches(t *testing.T, s *sinker, items []abstract.ChangeItem, random *rand.Rand) {
	for len(items) > 0 {
		size := 1 + random.Intn(40)
		if size > len(items) {
			size = len(items)
		}
		batch := make([]abstract.ChangeItem, size) // Push changes the items
		copy(batch, items[:size])
		require.NoError(t, s.Push(batch))
		items = items[size:]
	}
}

// Events delivered again must bring the target to the state of the source whatever state it is in already: one
// of an earlier moment (a restart from a saved position) or the final one (a switch from a mode which writes the
// current documents).
func TestPush_PatchesConvergeOverNewerState(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			ctx := context.Background()
			random := rand.New(rand.NewSource(seed))
			s, target, source, namespace := patchTestSinker(t, ctx, "patch_replay")
			s.config.OrderedWrites = seed%3 == 0
			items := replayEvents(t, ctx, source, namespace, random, 250)
			require.NotEmpty(t, items)
			expected := documentsByID(t, ctx, source)
			patches, truncations := 0, 0
			for i := range items {
				if items[i].Kind == abstract.MongoUpdateDocumentKind {
					patches++
					if len(items[i].ColumnValues[3].([]TruncatedArray)) > 0 {
						truncations++
					}
				}
			}
			require.NotZero(t, patches)

			// from scratch
			pushInBatches(t, s, items, random)
			require.Equal(t, expected, documentsByID(t, ctx, target), "from scratch")

			// all over again, the target has the final state
			pushInBatches(t, s, items, random)
			require.Equal(t, expected, documentsByID(t, ctx, target), "over the final state")

			// from a position behind the state of the target
			for attempt := 0; attempt < 3; attempt++ {
				_, err := target.DeleteMany(ctx, bson.D{})
				require.NoError(t, err)
				reached := 1 + random.Intn(len(items))
				restart := random.Intn(reached)
				pushInBatches(t, s, items[:reached], random)
				pushInBatches(t, s, items[restart:], random)
				require.Equal(t, expected, documentsByID(t, ctx, target), "items[:%d], then items[%d:]", reached, restart)
			}
			t.Logf("%d events, %d patches (%d truncate arrays), %d reads of the source",
				len(items), patches, truncations, s.source.(*collectionSource).finds.Load())
		})
	}
}

func TestNewSourceDocuments(t *testing.T) {
	source := &MongoSource{}
	one := []MongoCollection{{DatabaseName: "a", CollectionName: "c"}, {DatabaseName: "a", CollectionName: "d"}}
	two := append([]MongoCollection{{DatabaseName: "b", CollectionName: "c"}}, one...)

	// the target keeps the databases of the source
	require.NotNil(t, newSourceDocuments(source, nil, &MongoDestination{}, nil))
	require.NotNil(t, newSourceDocuments(source, two, &MongoDestination{}, nil))

	// the target puts everything into one database: its source must be the only one
	merged := &MongoDestination{Database: "dst"}
	documents := newSourceDocuments(source, one, merged, nil)
	require.NotNil(t, documents)
	require.Equal(t, "a", documents.(*lazySourceDocuments).database)
	require.Nil(t, newSourceDocuments(source, two, merged, nil))
	require.Nil(t, newSourceDocuments(source, nil, merged, nil), "all the collections of the source")
	require.Nil(t, newSourceDocuments(source, []MongoCollection{{DatabaseName: "*", CollectionName: "c"}}, merged, nil))
}

// uniqueCycleEvents records a legal unique-value swap through the source's real change stream.
func uniqueCycleEvents(
	t *testing.T,
	ctx context.Context,
	target, source *mongo_driver.Collection,
	namespace Namespace,
	tenant int32,
) []abstract.ChangeItem {
	initial := []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: tenant}, {Key: "u", Value: 1}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: tenant}, {Key: "u", Value: 3}},
	}
	for _, collection := range []*mongo_driver.Collection{target, source} {
		_, err := collection.Indexes().CreateOne(ctx, mongo_driver.IndexModel{
			Keys:    bson.D{{Key: "tenant", Value: 1}, {Key: "u", Value: 1}},
			Options: mongo_options.Index().SetUnique(true),
		})
		require.NoError(t, err)
		_, err = collection.InsertMany(ctx, initial)
		require.NoError(t, err)
	}

	stream, err := source.Watch(ctx, mongo_driver.Pipeline{},
		mongo_options.ChangeStream().SetFullDocument(mongo_options.Default).SetMaxAwaitTime(time.Second))
	require.NoError(t, err)
	defer func() { _ = stream.Close(context.Background()) }()

	updates := []struct {
		id string
		u  int
	}{{"a", 2}, {"b", 1}, {"a", 3}}
	for _, update := range updates {
		result, err := source.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: update.id}, {Key: "tenant", Value: tenant}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "u", Value: update.u}}}},
		)
		require.NoError(t, err)
		require.EqualValues(t, 1, result.MatchedCount)
		require.EqualValues(t, 1, result.ModifiedCount)
	}

	converter := &mongoSource{config: &MongoSource{IsHomo: true}}
	items := make([]abstract.ChangeItem, 0, len(updates))
	for _, update := range updates {
		require.True(t, stream.Next(ctx), "missing source update %s->%d: %v", update.id, update.u, stream.Err())
		var event ChangeEvent
		require.NoError(t, stream.Decode(&event))
		require.Equal(t, OperationTypeUpdate, event.OperationType)
		require.Equal(t, update.id, event.DocumentKey.ID)
		require.NotNil(t, event.UpdateDescription)
		require.Equal(t, bson.D{{Key: "u", Value: int32(update.u)}}, event.UpdateDescription.UpdatedFields)
		item, err := converter.makeChangeItem(&puChangeEvent{
			changeEvent: changeEvent{event: &event, size: len(stream.Current), decodeTime: 0},
		})
		require.NoError(t, err)
		require.Equal(t, abstract.MongoUpdateDocumentKind, item.Kind)
		items = append(items, item)
	}
	return inCollection(namespace, items...)
}

// shardedPatchTestSinker creates two tenant ranges owned by distinct real shards.
func shardedPatchTestSinker(
	t *testing.T, ctx context.Context, name string,
) (*sinker, *mongo_driver.Collection, *mongo_driver.Collection, Namespace) {
	if os.Getenv("MONGO_TEST_SHARDED") != "1" {
		t.Skip("requires MONGO_TEST_SHARDED=1 and two-shard mongos; ordinary CI has preparation coverage")
	}
	s, target, source, namespace := patchTestSinker(t, ctx, name)
	var hello struct {
		Msg string `bson:"msg"`
	}
	admin := s.client.Database("admin")
	require.NoError(t, admin.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello))
	require.Equal(t, "isdbgrid", hello.Msg, "MONGO_TEST_SHARDED requires an actual mongos endpoint")
	var shards struct {
		Shards []struct {
			ID string `bson:"_id"`
		} `bson:"shards"`
	}
	require.NoError(t, admin.RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&shards))
	require.GreaterOrEqual(t, len(shards.Shards), 2, "two real shards are required to exercise _id routing")
	require.NoError(t, admin.RunCommand(ctx, bson.D{{Key: "enableSharding", Value: namespace.Database}}).Err())
	_, err := target.Indexes().CreateOne(ctx, mongo_driver.IndexModel{Keys: bson.D{{Key: "tenant", Value: 1}}})
	require.NoError(t, err)
	require.NoError(t, admin.RunCommand(ctx, bson.D{
		{Key: "shardCollection", Value: namespace.GetFullName()},
		{Key: "key", Value: bson.D{{Key: "tenant", Value: 1}}},
	}).Err())
	key, err := GetShardingKey(ctx, s.client, namespace)
	require.NoError(t, err)
	require.NotNil(t, key, "the sink must discover the target's real sharding metadata")
	require.False(t, key.IsTrivial())
	require.Equal(t, []string{"tenant"}, key.Fields())

	var metadata struct {
		UUID primitive.Binary `bson:"uuid"`
	}
	config := s.client.Database("config")
	require.NoError(t, config.Collection("collections").FindOne(ctx,
		bson.D{{Key: "_id", Value: namespace.GetFullName()}}).Decode(&metadata))
	chunkFilter := bson.D{{Key: "uuid", Value: metadata.UUID}}
	var initialChunk struct {
		Shard string `bson:"shard"`
	}
	require.NoError(t, config.Collection("chunks").FindOne(ctx, chunkFilter).Decode(&initialChunk))
	otherShard := ""
	for _, shard := range shards.Shards {
		if shard.ID != initialChunk.Shard {
			otherShard = shard.ID
			break
		}
	}
	require.NotEmpty(t, otherShard)
	require.NoError(t, admin.RunCommand(ctx, bson.D{
		{Key: "split", Value: namespace.GetFullName()},
		{Key: "middle", Value: bson.D{{Key: "tenant", Value: 50}}},
	}).Err())
	require.NoError(t, admin.RunCommand(ctx, bson.D{
		{Key: "moveChunk", Value: namespace.GetFullName()},
		{Key: "find", Value: bson.D{{Key: "tenant", Value: 99}}},
		{Key: "to", Value: otherShard},
		{Key: "_waitForDelete", Value: true},
	}).Err())
	cursor, err := config.Collection("chunks").Find(ctx, chunkFilter)
	require.NoError(t, err)
	var chunks []struct {
		Shard string `bson:"shard"`
	}
	require.NoError(t, cursor.All(ctx, &chunks))
	owners := map[string]bool{}
	for _, chunk := range chunks {
		owners[chunk.Shard] = true
	}
	require.Len(t, owners, 2, "both shards must own ranges before testing _id-only patch routing")
	require.True(t, owners[initialChunk.Shard])
	require.True(t, owners[otherShard])
	return s, target, source, namespace
}

func TestPush_ShardedPatchUniqueValueCycle(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		for _, tenant := range []int32{1, 99} {
			t.Run(fmt.Sprintf("ordered=%v/tenant=%d", ordered, tenant), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				s, target, source, namespace := shardedPatchTestSinker(t, ctx,
					fmt.Sprintf("patch_unique_cycle_%v_%d", ordered, tenant))
				s.config.OrderedWrites = ordered
				items := uniqueCycleEvents(t, ctx, target, source, namespace, tenant)
				require.Len(t, items, 3)
				expected := documentsByID(t, ctx, source)
				firstErr := s.Push(append([]abstract.ChangeItem(nil), items...))
				firstState := documentsByID(t, ctx, target)
				t.Logf("first Push: %v; target: %v", firstErr, firstState)
				replayErr := s.Push(append([]abstract.ChangeItem(nil), items...))
				t.Logf("repeated Push: %v; target: %v", replayErr, documentsByID(t, ctx, target))
				require.NoError(t, firstErr)
				require.Equal(t, expected, firstState, "a successful first Push must apply the whole history")
				require.NoError(t, replayErr)
				require.Equal(t, expected, documentsByID(t, ctx, target))
				if ordered {
					require.Zero(t, s.source.(*collectionSource).finds.Load(), "ordered legal patches need no source read")
				}
			})
		}
	}
}

func TestResolvePatches_PreservesSafeEventsAndOrigins(t *testing.T) {
	ctx := context.Background()
	s, _, source, namespace := patchTestSinker(t, ctx, "patch_resolve_selective")
	_, err := source.InsertOne(ctx,
		bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: 99}, {Key: "u", Value: 3}})
	require.NoError(t, err)
	items := inCollection(namespace,
		patchItem("a", bson.D{{Key: "u", Value: 2}}, nil, nil),
		patchItem("a", bson.D{{Key: "tenant", Value: 1}}, nil, nil),
		patchItem("gone", bson.D{{Key: "tenant", Value: 1}}, nil, nil),
		patchItem("a", bson.D{{Key: "u", Value: 3}}, nil, nil),
	)
	resolved, origin, err := s.resolvePatches(ctx, namespace, items, []string{"tenant"})
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 3}, origin)
	require.Len(t, resolved, 3)
	require.Equal(t, items[0], resolved[0])
	require.Equal(t, items[3], resolved[2])
	require.Equal(t, abstract.UpdateKind, resolved[1].Kind)
	require.EqualValues(t, 1, s.source.(*collectionSource).finds.Load())

	s.source = nil
	safe := []abstract.ChangeItem{items[0], items[3]}
	preserved, origin, err := s.resolvePatches(ctx, namespace, safe, []string{"tenant"})
	require.NoError(t, err, "safe patches must not depend on source availability")
	require.Nil(t, origin)
	require.Equal(t, safe, preserved)
}

func TestPush_RefetchWithoutConfigRead(t *testing.T) {
	if os.Getenv("MONGO_TEST_AUTH") != "1" {
		t.Skip("requires MONGO_TEST_AUTH=1 and an auth-enabled ordinary replica with a root fixture user")
	}
	for _, ordered := range []bool{false, true} {
		for _, missing := range []bool{false, true} {
			t.Run(fmt.Sprintf("ordered=%v/missing=%v", ordered, missing), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				name := fmt.Sprintf("patch_no_config_%v_%v", ordered, missing)
				s, target, source, namespace := patchTestSinker(t, ctx, name)
				var topology struct {
					Msg     string `bson:"msg"`
					SetName string `bson:"setName"`
				}
				require.NoError(t, s.client.Database("admin").RunCommand(ctx,
					bson.D{{Key: "isMaster", Value: 1}}).Decode(&topology))
				if topology.Msg == "isdbgrid" {
					t.Skip("ordinary-replica privilege test; sharded metadata failures need separate coverage")
				}
				require.NotEmpty(t, topology.SetName, "this privilege case requires an ordinary replica set")
				_, err := source.InsertOne(ctx,
					bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: bson.D{{Key: "x", Value: 7}}}})
				require.NoError(t, err)
				if !missing {
					_, err = target.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: 5}})
					require.NoError(t, err)
				}

				username := name + "_writer"
				database := s.client.Database(namespace.Database)
				_ = database.RunCommand(ctx, bson.D{{Key: "dropUser", Value: username}}).Err()
				require.NoError(t, database.RunCommand(ctx, bson.D{
					{Key: "createUser", Value: username}, {Key: "pwd", Value: "test-only-refetch-password"},
					{Key: "roles", Value: bson.A{bson.D{
						{Key: "role", Value: "readWrite"}, {Key: "db", Value: namespace.Database},
					}}},
				}).Err())
				t.Cleanup(func() {
					_ = database.RunCommand(context.Background(), bson.D{{Key: "dropUser", Value: username}}).Err()
				})
				opts := s.config.ConnectionOptions(nil)
				opts.User, opts.Password, opts.AuthSource = username, "test-only-refetch-password", namespace.Database
				client, err := Connect(ctx, opts, s.logger)
				require.NoError(t, err)
				t.Cleanup(func() { _ = client.Close(context.Background()) })
				denied := client.Database("config").Collection("collections").FindOne(ctx, bson.D{}).Err()
				var deniedCommand mongo_driver.CommandError
				require.ErrorAs(t, denied, &deniedCommand)
				require.EqualValues(t, 13, deniedCommand.Code, "the test writer must lack config read privileges")

				s.client, s.config.OrderedWrites = client, ordered
				pushErr := s.Push(inCollection(namespace, patchItem("a", bson.D{{Key: "n.x", Value: 1}}, nil, nil)))
				t.Logf("ordinary refetch with database-scoped writer: %v", pushErr)
				require.NoError(t, pushErr)
				require.NotZero(t, s.source.(*collectionSource).finds.Load(), "missing/mismatch recovery must read source")
				require.Equal(t, documentsByID(t, ctx, source), documentsByID(t, ctx, target))
			})
		}
	}
}

// failNextMetadataRead affects one local test client; other clients and internal mongos commands are excluded.
func failNextMetadataRead(t *testing.T, ctx context.Context, s *sinker) *atomic.Int64 {
	t.Helper()
	require.Equal(t, "1", os.Getenv("MONGO_TEST_FAILPOINTS"), "local failpoints require explicit opt-in")
	require.Empty(t, s.config.ClusterID)
	for _, host := range s.config.Hosts {
		require.Contains(t, []string{"localhost", "127.0.0.1", "::1"}, host, "failpoints require a local fixture")
	}
	admin := s.client.Database("admin")
	var hello struct {
		Msg string `bson:"msg"`
	}
	require.NoError(t, admin.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&hello))
	require.Equal(t, "isdbgrid", hello.Msg, "this failpoint test requires local mongos")

	connection := s.config.ConnectionOptions(nil)
	opts, err := DriverConnectionOptions(&connection)
	require.NoError(t, err)
	appName := fmt.Sprintf("mongo-metadata-test-%d", time.Now().UnixNano())
	var metadataRequest atomic.Int64
	failures := &atomic.Int64{}
	opts.SetAppName(appName).SetRetryReads(false).SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, command *event.CommandStartedEvent) {
			if command.CommandName == "find" && command.DatabaseName == "config" &&
				command.Command.Lookup("find").StringValue() == "collections" {
				metadataRequest.Store(command.RequestID)
			}
		},
		Succeeded: nil,
		Failed: func(_ context.Context, command *event.CommandFailedEvent) {
			if command.CommandName == "find" && command.RequestID == metadataRequest.Load() {
				failures.Add(1)
			}
		},
	})
	client, err := newClient(ctx, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	wrapped := *s.client
	wrapped.Client = client
	s.client = &wrapped

	require.NoError(t, admin.RunCommand(ctx, bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{{Key: "times", Value: 1}}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{"find"}}, {Key: "errorCode", Value: 13},
			{Key: "appName", Value: appName}, {Key: "failInternalCommands", Value: false},
		}},
	}).Err())
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, admin.RunCommand(cleanupCtx, bson.D{
			{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"},
		}).Err())
	})
	return failures
}

func TestPush_ShardedPatchMetadataFailure(t *testing.T) {
	if os.Getenv("MONGO_TEST_FAILPOINTS") != "1" {
		t.Skip("requires MONGO_TEST_FAILPOINTS=1 on local two-shard mongos with enableTestCommands=1")
	}
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("mixed=%v", mixed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s, target, source, namespace := shardedPatchTestSinker(t, ctx, fmt.Sprintf("patch_metadata_failure_%v", mixed))
			_, err := target.Indexes().CreateOne(ctx, mongo_driver.IndexModel{
				Keys:    bson.D{{Key: "tenant", Value: 1}, {Key: "u", Value: 1}},
				Options: mongo_options.Index().SetUnique(true),
			})
			require.NoError(t, err)
			_, err = target.InsertOne(ctx, bson.D{
				{Key: "_id", Value: "a"}, {Key: "tenant", Value: 1}, {Key: "u", Value: 7}, {Key: "n", Value: 5},
			})
			require.NoError(t, err)
			current := bson.D{
				{Key: "_id", Value: "a"}, {Key: "tenant", Value: 99}, {Key: "u", Value: 7},
				{Key: "n", Value: bson.D{{Key: "x", Value: 2}}},
			}
			_, err = source.InsertOne(ctx, current)
			require.NoError(t, err)
			items := []abstract.ChangeItem{patchItem("a", bson.D{{Key: "n.x", Value: 1}}, nil, nil)}
			expectedTenant := int32(99)
			if mixed {
				historical := append(bson.D(nil), current...)
				historical[1].Value = 1
				items = append(items, replaceItem("a", historical))
				expectedTenant = 1
			}
			items = inCollection(namespace, items...)
			before := documentsByID(t, ctx, target)
			failures := failNextMetadataRead(t, ctx, s)
			pushErr := s.Push(append([]abstract.ChangeItem(nil), items...))
			failedState := documentsByID(t, ctx, target)
			count, err := target.CountDocuments(ctx, bson.D{{Key: "_id", Value: "a"}})
			require.NoError(t, err)
			t.Logf("Push after failed metadata lookup: %v; documents=%d; target=%v", pushErr, count, failedState)
			require.EqualValues(t, 1, failures.Load(), "the injected error must hit config.collections")
			require.Error(t, pushErr, "unknown metadata must fail before the first write")
			require.EqualValues(t, 1, count)
			require.Equal(t, before, failedState)
			require.Zero(t, s.source.(*collectionSource).finds.Load(), "failed grouping must not enter recovery")

			key, err := GetShardingKey(ctx, s.client, namespace)
			require.NoError(t, err, "the next metadata lookup must work after the one injected failure")
			require.NotNil(t, key)
			require.NoError(t, s.Push(items))
			count, err = target.CountDocuments(ctx, bson.D{{Key: "_id", Value: "a"}})
			require.NoError(t, err)
			require.EqualValues(t, 1, count, "fresh boundaries must not create a second copy under the old tenant")
			require.Equal(t, map[string]bson.M{
				"a": {"_id": "a", "tenant": expectedTenant, "u": int32(7), "n": bson.M{"x": int32(2)}},
			}, documentsByID(t, ctx, target))
		})
	}
}

func TestPush_MongosFullDocumentWithoutConfigRead(t *testing.T) {
	if os.Getenv("MONGO_TEST_FAILPOINTS") != "1" {
		t.Skip("requires MONGO_TEST_FAILPOINTS=1 on local mongos with enableTestCommands=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, target, namespace := testSinker(t, ctx, "full_document_no_config_read", false)
	require.NoError(t, s.client.Database(namespace.Database).CreateCollection(ctx, namespace.Collection))
	key, err := GetShardingKey(ctx, s.client, namespace)
	require.NoError(t, err)
	require.Nil(t, key, "this full-document target must be unsharded")
	failures := failNextMetadataRead(t, ctx, s)
	pushErr := s.Push(inCollection(namespace,
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: 7}}),
	))
	t.Logf("full-document insert through mongos with denied config read: %v", pushErr)
	require.EqualValues(t, 1, failures.Load(), "the metadata denial must actually occur")
	require.NoError(t, pushErr)
	require.Equal(t, map[string]bson.M{"a": {"_id": "a", "v": int32(7)}}, documentsByID(t, ctx, target))
}

func TestRetryCollectionOrdered_PatchRemainderMetadataFailure(t *testing.T) {
	if os.Getenv("MONGO_TEST_FAILPOINTS") != "1" {
		t.Skip("requires MONGO_TEST_FAILPOINTS=1 on local two-shard mongos with enableTestCommands=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, target, _, namespace := shardedPatchTestSinker(t, ctx, "patch_remainder_metadata_failure")
	_, err := target.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: 99}, {Key: "v", Value: 2}})
	require.NoError(t, err)
	before := documentsByID(t, ctx, target)
	// The preceding patch already refetched tenant99; only this historical full write remains pending.
	items := inCollection(namespace,
		replaceItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "tenant", Value: 1}, {Key: "v", Value: 3}}),
	)
	failures := failNextMetadataRead(t, ctx, s)
	err = s.retryCollectionOrdered(ctx, namespace, items, true)
	require.EqualValues(t, 1, failures.Load(), "the remaining full write must require successful metadata discovery")
	require.Error(t, err)
	require.Equal(t, before, documentsByID(t, ctx, target))
	require.NoError(t, s.retryCollectionOrdered(ctx, namespace, items, true))
	count, err := target.CountDocuments(ctx, bson.D{{Key: "_id", Value: "a"}})
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.Equal(t, map[string]bson.M{"a": {"_id": "a", "tenant": int32(1), "v": int32(3)}},
		documentsByID(t, ctx, target))
}
