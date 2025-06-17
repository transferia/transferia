//go:build !disable_mongo_provider

package mongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestIsTrivialKey(t *testing.T) {
	key1 := bson.D{{Key: "_id", Value: 1}}
	require.True(t, isTrivialKey(key1))
	key2 := bson.D{{Key: "_id.nested", Value: 1}}
	require.True(t, isTrivialKey(key2))
	key3 := bson.D{{Key: "_id.x", Value: 1}, {Key: "_id.y", Value: 1}}
	require.True(t, isTrivialKey(key3))
	key4 := bson.D{{Key: "_id.x", Value: 1}, {Key: "_id.y", Value: 1}, {Key: "some_key", Value: 1}}
	require.False(t, isTrivialKey(key4))
}

func TestContainsID(t *testing.T) {
	key1 := bson.D{{Key: "_id", Value: 1}}
	require.True(t, containsID(key1))
	key2 := bson.D{{Key: "_id.nested", Value: 1}}
	require.True(t, containsID(key2))
	key3 := bson.D{{Key: "_id.x", Value: 1}, {Key: "_id.y", Value: 1}}
	require.True(t, containsID(key3))
	key4 := bson.D{{Key: "_id.x", Value: 1}, {Key: "_id.y", Value: 1}, {Key: "some_key", Value: 1}}
	require.True(t, containsID(key4))
	key5 := bson.D{{Key: "key1", Value: 1}}
	require.False(t, containsID(key5))
}
