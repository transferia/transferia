package mongo

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func testDocumentID(id string) documentID {
	return documentID{Raw: id, String: id}
}

func testReplace(id string, v int) *mongo.ReplaceOneModel {
	return makeReplaceModel(bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "_id", Value: id}, {Key: "v", Value: v}})
}

func TestBulkSplitter(t *testing.T) {
	model := &mongo.UpdateOneModel{}
	id1 := documentID{Raw: "id1", String: "id1"}
	id2 := documentID{Raw: "id2", String: "id2"}
	splitter := newBulkSplitter(true)
	splitter.Add(model, id1, false)
	splitter.Add(model, id2, false)
	splitter.Add(model, id1, false)
	splitter.Add(model, id2, true)
	bulks := splitter.Get()
	require.Equal(t, len(bulks), 3)
	require.Equal(t, len(bulks[0]), 2)
	require.Equal(t, len(bulks[1]), 1)
	require.Equal(t, len(bulks[2]), 1)
}

func TestBulkSplitter_AllUniqueIDs_SingleBulk(t *testing.T) {
	model := &mongo.UpdateOneModel{}
	splitter := newBulkSplitter(true)
	N := 57
	for i := 0; i < N; i++ {
		idStr := fmt.Sprintf("id%d", i)
		id := documentID{Raw: idStr, String: idStr}
		splitter.Add(model, id, false)
	}
	bulks := splitter.Get()
	require.Len(t, bulks, 1)
	require.Len(t, bulks[0], N)
}

func TestBulkSplitter_FoldsRepeatedDocuments(t *testing.T) {
	splitter := newBulkSplitter(true)
	splitter.Add(makeInsertModel(bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: 1}}), testDocumentID("a"), false)
	splitter.Add(testReplace("b", 1), testDocumentID("b"), false)
	splitter.Add(testReplace("a", 2), testDocumentID("a"), false)                               // a replace supersedes the insert
	splitter.Add(makeDeleteModel(bson.D{{Key: "_id", Value: "b"}}), testDocumentID("b"), false) // a delete supersedes the replace
	splitter.Add(makeInsertModel(bson.D{{Key: "_id", Value: "c"}, {Key: "v", Value: 1}}), testDocumentID("c"), false)
	splitter.Add(makeInsertModel(bson.D{{Key: "_id", Value: "c"}, {Key: "v", Value: 2}}), testDocumentID("c"), false) // a repeated insert becomes an upsert
	splitter.Add(testReplace("a", 3), testDocumentID("a"), false)

	bulks := splitter.Get()
	require.Len(t, bulks, 1)
	require.Len(t, bulks[0], 3)
	require.Equal(t, 4, splitter.Folded())

	a, ok := bulks[0][0].(*mongo.ReplaceOneModel)
	require.True(t, ok)
	require.Equal(t, bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: 3}}, a.Replacement)
	require.IsType(t, &mongo.DeleteOneModel{}, bulks[0][1])
	c, ok := bulks[0][2].(*mongo.ReplaceOneModel)
	require.True(t, ok)
	require.Equal(t, bson.D{{Key: "_id", Value: "c"}, {Key: "v", Value: 2}}, c.Replacement)
	require.True(t, *c.Upsert)
}

// A partial update depends on the state before it and is not folded; a full-document write after it is.
func TestBulkSplitter_PartialUpdateCutsBulk(t *testing.T) {
	id := testDocumentID("a")
	filter := bson.D{{Key: "_id", Value: "a"}}
	splitter := newBulkSplitter(true)
	splitter.Add(testReplace("a", 1), id, false)
	splitter.Add(makeUpdateModel(filter, bson.D{{Key: "v", Value: 2}}, nil), id, false)
	splitter.Add(testReplace("a", 3), id, false)

	bulks := splitter.Get()
	require.Len(t, bulks, 2)
	require.Len(t, bulks[0], 1)
	require.Len(t, bulks[1], 1)
	require.IsType(t, &mongo.ReplaceOneModel{}, bulks[1][0])
	require.Equal(t, 1, splitter.Folded())
}

func TestBulkSplitter_HotDocumentKeepsOneBulk(t *testing.T) {
	splitter := newBulkSplitter(true)
	const n = 100
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("id%d", i)
		splitter.Add(testReplace(id, i), testDocumentID(id), false)
		splitter.Add(testReplace("hot", i), testDocumentID("hot"), false)
	}
	bulks := splitter.Get()
	require.Len(t, bulks, 1)
	require.Len(t, bulks[0], n+1)
	require.Equal(t, n-1, splitter.Folded())
}

// With fold off (OrderedWrites) a repeated document cuts the bulk, as it always did: the application order across
// documents must stay the source order.
func TestBulkSplitter_NoFold_RepeatedDocumentCutsBulk(t *testing.T) {
	splitter := newBulkSplitter(false)
	splitter.Add(testReplace("a", 1), testDocumentID("a"), false)
	splitter.Add(testReplace("b", 1), testDocumentID("b"), false)
	splitter.Add(testReplace("a", 2), testDocumentID("a"), false)

	bulks := splitter.Get()
	require.Len(t, bulks, 2)
	require.Len(t, bulks[0], 2)
	require.Len(t, bulks[1], 1)
	require.Equal(t, 0, splitter.Folded())
}

// An isolated write (shard key change) of a document that was just folded gets its own bulk after the folded one.
func TestBulkSplitter_FoldThenIsolated(t *testing.T) {
	splitter := newBulkSplitter(true)
	splitter.Add(testReplace("a", 1), testDocumentID("a"), false)
	splitter.Add(testReplace("a", 2), testDocumentID("a"), false)
	splitter.Add(testReplace("a", 3), testDocumentID("a"), true)
	splitter.Add(testReplace("a", 4), testDocumentID("a"), false)

	bulks := splitter.Get()
	require.Len(t, bulks, 3)
	require.Equal(t, 1, splitter.Folded())
	for i, v := range []int{2, 3, 4} {
		require.Len(t, bulks[i], 1)
		require.Equal(t, bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: v}}, bulks[i][0].(*mongo.ReplaceOneModel).Replacement)
	}
}

// In a collection sharded by a key other than _id, a document recreated under another key is located by the new
// key: its upsert would leave the copy on the old shard in place, so it does not fold over the delete.
func TestBulkSplitter_DeleteThenUpsertByShardKeyCutsBulk(t *testing.T) {
	id := testDocumentID("a")
	keyFields := []string{"k"}
	oldKey := makeDocumentFilter("a", bson.M{"k": "old"}, keyFields)
	newKey := makeDocumentFilter("a", bson.M{"k": "new"}, keyFields)
	splitter := newBulkSplitter(true)
	splitter.Add(makeReplaceModel(oldKey, bson.D{{Key: "_id", Value: "a"}, {Key: "k", Value: "old"}}), id, false)
	splitter.Add(makeDeleteModel(makeDocumentFilter("a", nil, nil)), id, false)
	splitter.Add(makeReplaceModel(newKey, bson.D{{Key: "_id", Value: "a"}, {Key: "k", Value: "new"}}), id, false)

	bulks := splitter.Get()
	require.Len(t, bulks, 2)
	require.Len(t, bulks[0], 1)
	require.IsType(t, &mongo.DeleteOneModel{}, bulks[0][0])
	require.Len(t, bulks[1], 1)
	require.Equal(t, newKey, bulks[1][0].(*mongo.ReplaceOneModel).Filter)
	require.Equal(t, 1, splitter.Folded())
}

// An upsert by _id alone replaces whatever the delete would have removed, so it folds over the delete.
func TestBulkSplitter_DeleteThenUpsertByIDFolds(t *testing.T) {
	splitter := newBulkSplitter(true)
	splitter.Add(makeDeleteModel(bson.D{{Key: "_id", Value: "a"}}), testDocumentID("a"), false)
	splitter.Add(testReplace("a", 1), testDocumentID("a"), false)
	splitter.Add(makeDeleteModel(bson.D{{Key: "_id", Value: "b"}}), testDocumentID("b"), false)
	splitter.Add(makeInsertModel(bson.D{{Key: "_id", Value: "b"}, {Key: "v", Value: 1}}), testDocumentID("b"), false)

	bulks := splitter.Get()
	require.Len(t, bulks, 1)
	require.Len(t, bulks[0], 2)
	require.Equal(t, 2, splitter.Folded())
	for _, write := range bulks[0] {
		replace, ok := write.(*mongo.ReplaceOneModel)
		require.True(t, ok)
		require.True(t, *replace.Upsert)
	}
}
