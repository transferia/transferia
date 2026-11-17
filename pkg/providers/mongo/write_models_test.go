package mongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
)

func TestInsertToUpsertFallback(t *testing.T) {
	document := bson.D{{Key: "_id", Value: 1}, {Key: "k", Value: "shard"}, {Key: "v", Value: 2}}

	model := insertToUpsertFallback(makeInsertModel(document))
	replace, ok := model.(*mongo_driver.ReplaceOneModel)
	require.True(t, ok)
	require.Equal(t, bson.D{{Key: "_id", Value: 1}}, replace.Filter)
	require.Equal(t, document, replace.Replacement)
	require.True(t, *replace.Upsert)

	require.Nil(t, insertToUpsertFallback(makeReplaceModel(bson.D{{Key: "_id", Value: 1}}, document)))
	require.Nil(t, insertToUpsertFallback(makeInsertModel(bson.D{{Key: "v", Value: 2}})), "no root _id")
}

func updatesOf(t *testing.T, models []mongo_driver.WriteModel) []bson.D {
	updates := make([]bson.D, 0, len(models))
	for _, model := range models {
		update, ok := model.(*mongo_driver.UpdateOneModel)
		require.True(t, ok)
		require.Equal(t, bson.D{{Key: "_id", Value: 1}}, update.Filter)
		require.Nil(t, update.Upsert, "a patch must not create a document")
		updates = append(updates, update.Update.(bson.D))
	}
	return updates
}

func TestMakeUpdateModels(t *testing.T) {
	filter := bson.D{{Key: "_id", Value: 1}}
	slice := func(size int) bson.D {
		return bson.D{{Key: "$each", Value: bson.A{}}, {Key: "$slice", Value: size}}
	}

	t.Run("nothing changed", func(t *testing.T) {
		require.Empty(t, makeUpdateModels(filter, nil, nil, nil))
		require.Empty(t, makeUpdateModels(filter, bson.D{}, []string{}, []TruncatedArray{}))
	})

	t.Run("set and unset", func(t *testing.T) {
		updates := updatesOf(t, makeUpdateModels(filter, bson.D{{Key: "a.b", Value: 1}}, []string{"c"}, nil))
		require.Equal(t, []bson.D{{
			{Key: "$set", Value: bson.D{{Key: "a.b", Value: 1}}},
			{Key: "$unset", Value: bson.D{{Key: "c", Value: ""}}},
		}}, updates)
	})

	t.Run("truncation alone", func(t *testing.T) {
		updates := updatesOf(t, makeUpdateModels(filter, nil, nil, []TruncatedArray{{Field: "arr", NewSize: 2}}))
		require.Equal(t, []bson.D{{
			{Key: "$push", Value: bson.D{{Key: "arr", Value: slice(2)}}},
		}}, updates)
	})

	t.Run("truncation with writes elsewhere", func(t *testing.T) {
		// "arr2" and "ar" only look like "arr"
		updates := updatesOf(t, makeUpdateModels(filter,
			bson.D{{Key: "arr2.0", Value: 1}, {Key: "ar", Value: 2}}, []string{"x.arr"},
			[]TruncatedArray{{Field: "arr", NewSize: 0}}))
		require.Equal(t, []bson.D{{
			{Key: "$push", Value: bson.D{{Key: "arr", Value: slice(0)}}},
			{Key: "$set", Value: bson.D{{Key: "arr2.0", Value: 1}, {Key: "ar", Value: 2}}},
			{Key: "$unset", Value: bson.D{{Key: "x.arr", Value: ""}}},
		}}, updates)
	})

	t.Run("truncation with writes below the array", func(t *testing.T) {
		updates := updatesOf(t, makeUpdateModels(filter,
			bson.D{{Key: "n.arr.0", Value: 1}, {Key: "v", Value: 2}, {Key: "n.arr.1.x", Value: 3}},
			[]string{"n.arr.1.y", "w"},
			[]TruncatedArray{{Field: "n.arr", NewSize: 2}, {Field: "other", NewSize: 1}}))
		require.Equal(t, []bson.D{
			{
				{Key: "$push", Value: bson.D{{Key: "n.arr", Value: slice(2)}, {Key: "other", Value: slice(1)}}},
				{Key: "$set", Value: bson.D{{Key: "v", Value: 2}}},
				{Key: "$unset", Value: bson.D{{Key: "w", Value: ""}}},
			},
			{
				{Key: "$set", Value: bson.D{{Key: "n.arr.0", Value: 1}, {Key: "n.arr.1.x", Value: 3}}},
				{Key: "$unset", Value: bson.D{{Key: "n.arr.1.y", Value: ""}}},
			},
		}, updates)
	})

	t.Run("truncation of nested arrays", func(t *testing.T) {
		updates := updatesOf(t, makeUpdateModels(filter,
			bson.D{{Key: "a.0.1", Value: 7}, {Key: "a.5", Value: 8}}, nil,
			[]TruncatedArray{{Field: "a.0", NewSize: 2}, {Field: "a", NewSize: 9}, {Field: "a.1", NewSize: 3}}))
		require.Equal(t, []bson.D{
			{{Key: "$push", Value: bson.D{{Key: "a", Value: slice(9)}}}},
			{
				{Key: "$push", Value: bson.D{{Key: "a.0", Value: slice(2)}, {Key: "a.1", Value: slice(3)}}},
				{Key: "$set", Value: bson.D{{Key: "a.5", Value: 8}}},
			},
			{{Key: "$set", Value: bson.D{{Key: "a.0.1", Value: 7}}}},
		}, updates)
	})
}
