//go:build !disable_mongo_provider

package mongo

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestHasDiff(t *testing.T) {
	doc1 := bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
			"lvl13": nil,
		},
	}
	doc2 := bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value2",
		},
	}
	require.False(t, hasDiff(doc1, doc2, []string{"root.lvl1.lvl2"}))
	require.True(t, hasDiff(doc1, doc2, []string{"root.lvl11"}))
	require.False(t, hasDiff(doc1, doc2, []string{"root.lvl13"}))
	require.True(t, hasDiff(doc1, doc2, []string{"root.lvl13", "root.lvl1.lvl2", "root.lvl11"}))
	require.False(t, hasDiff(doc1, doc2, []string{"root.lvl1"}))
	require.True(t, hasDiff(doc1, doc2, []string{"root"}))
}

func TestUpdateDocument(t *testing.T) {
	doc := bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
			"lvl13": nil,
			"lvl14": []string{"abc"},
		},
	}
	patch := map[string]any{
		"root.lvl13":      "value13",
		"root.lvl1.lvl21": "value21",
		"root.lvl11":      "value11",
		"root1":           "value",
		"root.lvl14":      "value14",
	}
	updatedDoc := bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2":  "value",
				"lvl21": "value21",
			},
			"lvl11": "value11",
			"lvl13": "value13",
			"lvl14": "value14",
		},
		"root1": "value",
	}

	result, err := updateDocument(doc, patch)
	require.NoError(t, err)
	require.Equal(t, result, updatedDoc)
	require.Equal(t, doc["root"].(bson.M)["lvl11"], "value1")
}

func TestCopyDocument(t *testing.T) {
	id := primitive.NewObjectID()
	doc1 := bson.M{
		"root": bson.M{
			"oid": id,
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
			"lvl13": nil,
			"lvl14": []string{"abc"},
		},
	}
	copyDoc, err := copyDocument(doc1)
	require.NoError(t, err)

	doc2, ok := copyDoc.(bson.M)
	require.True(t, ok)
	doc2["root"].(bson.M)["lvl14"].([]string)[0] = "efg"
	require.Equal(t, doc1["root"].(bson.M)["lvl14"].([]string)[0], "abc")
	require.Equal(t, len(doc2["root"].(bson.M)), 5)
}

func BenchmarkCopyDocument(b *testing.B) {
	id := primitive.NewObjectID()
	doc1 := bson.M{
		"root": bson.M{
			"oid": id,
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
			"lvl13": nil,
			"lvl14": []string{"abc"},
		},
	}
	b.SetBytes(int64(util.DeepSizeof(doc1)))
	for n := 0; n < b.N; n++ {
		_, err := copyDocument(doc1)
		require.NoError(b, err)
	}
	b.ReportAllocs()
}

func BenchmarkCopyDocumentParallel(b *testing.B) {
	id := primitive.NewObjectID()
	doc1 := bson.M{
		"root": bson.M{
			"oid": id,
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
			"lvl13": nil,
			"lvl14": []string{"abc"},
		},
	}
	b.SetBytes(int64(util.DeepSizeof(doc1)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := copyDocument(doc1)
			require.NoError(b, err)
		}
	})
	b.ReportAllocs()
}

func TestEqualAny(t *testing.T) {
	var docM1, docM11, docM2, docD1, docD2 any
	docM1 = bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
			"lvl13": nil,
			"lvl14": []string{"abc"},
		},
	}
	docM11 = bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
			"lvl13": nil,
			"lvl14": []string{"abc"},
		},
	}
	docM2 = bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2":  "value",
				"lvl21": "value21",
			},
			"lvl11": "value11",
			"lvl13": "value13",
			"lvl14": "value14",
		},
		"root1": "value",
	}
	require.True(t, reflect.DeepEqual(docM1, docM1))
	require.True(t, reflect.DeepEqual(docM1, docM11))
	require.True(t, reflect.DeepEqual(docM2, docM2))
	require.False(t, reflect.DeepEqual(docM1, docM2))

	docD1 = bson.D{
		{Key: "root", Value: bson.D{
			{Key: "lvl1", Value: bson.D{
				{Key: "lvl2", Value: "value"},
			}},
			{Key: "lvl11", Value: "value1"},
			{Key: "lvl13", Value: nil},
			{Key: "lvl14", Value: []string{"abc"}},
		}},
	}
	docD2 = bson.D{
		{Key: "root", Value: bson.D{
			{Key: "lvl1", Value: bson.D{
				{Key: "lvl2", Value: "value"},
				{Key: "lvl21", Value: "value21"},
			}},
			{Key: "lvl11", Value: "value11"},
			{Key: "lvl13", Value: "value13"},
			{Key: "lvl14", Value: "value14"},
		}},
		{Key: "root1", Value: "value"},
	}

	require.True(t, reflect.DeepEqual(docD1, docD1))
	require.True(t, reflect.DeepEqual(docD2, docD2))
	require.False(t, reflect.DeepEqual(docD1, docD2))

	require.False(t, reflect.DeepEqual(docM1, docD1))
	require.True(t, reflect.DeepEqual([]string{"abc"}, []string{"abc"}))
}
