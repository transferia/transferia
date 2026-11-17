package mongo

import (
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestUpdateDescription_HasDottedFieldName(t *testing.T) {
	paths := func(paths bson.D) *UpdateDescription {
		return &UpdateDescription{
			UpdatedFields:      nil,
			RemovedFields:      nil,
			TruncatedArrays:    nil,
			DisambiguatedPaths: paths,
		}
	}
	require.False(t, paths(nil).HasDottedFieldName())
	// a numeric name of a field (not an index of an array) is the same path for an update operator
	require.False(t, paths(bson.D{{Key: "m.0", Value: bson.A{"m", "0"}}}).HasDottedFieldName())
	require.False(t, paths(bson.D{{Key: "arr.0.x", Value: bson.A{"arr", int32(0), "x"}}}).HasDottedFieldName())
	require.True(t, paths(bson.D{{Key: "dot.ted", Value: bson.A{"dot.ted"}}}).HasDottedFieldName())
	require.True(t, paths(bson.D{
		{Key: "m.0", Value: bson.A{"m", "0"}},
		{Key: "a.b.c", Value: bson.A{"a", "b.c"}},
	}).HasDottedFieldName())
}

func TestUpdateDocumentObstacle(t *testing.T) {
	homo := &MongoSource{IsHomo: true}
	version := func(v string) *semver.Version {
		parsed := semver.MustParse(v)
		return &parsed
	}
	require.Empty(t, updateDocumentObstacle(homo, version("4.4.2")))
	require.NotEmpty(t, updateDocumentObstacle(homo, version("5.0.0")))
	require.NotEmpty(t, updateDocumentObstacle(homo, version("6.0.18")))
	require.Empty(t, updateDocumentObstacle(homo, version("6.1.0")))
	require.Empty(t, updateDocumentObstacle(homo, version("8.0.29")))
	require.NotEmpty(t, updateDocumentObstacle(homo, nil))
	require.NotEmpty(t, updateDocumentObstacle(&MongoSource{IsHomo: false}, version("8.0.29")))
}
