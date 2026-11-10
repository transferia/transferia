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
