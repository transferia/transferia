//go:build !disable_mongo_provider

package mongo

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestExtBson(t *testing.T) {
	var x DExtension
	_, ok := x.Map()["a"]
	require.False(t, ok)
	x.SetKey("a", 1)
	val, ok := x.Map()["a"]
	require.True(t, ok)
	require.Equal(t, 1, val)
}

func TestGetValueByPath(t *testing.T) {
	docM := bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
		},
	}
	result, ok := GetValueByPath(docM, "root.lvl1.lvl2")
	require.True(t, ok)
	require.Equal(t, result, "value")

	_, ok = GetValueByPath(docM, "root.lvl1.lvl3")
	require.False(t, ok)

	_, ok = GetValueByPath(docM, "root.lvl11.lvl3")
	require.False(t, ok)

	docD := bson.D{
		{Key: "root", Value: bson.D{
			{Key: "lvl1", Value: bson.D{{Key: "lvl2", Value: "value"}}},
			{Key: "lvl11", Value: "value1"},
		}},
	}
	result, ok = GetValueByPath(docD, "root.lvl1.lvl2")
	require.True(t, ok)
	require.Equal(t, result, "value")

	_, ok = GetValueByPath(docD, "root.lvl1.lvl3")
	require.False(t, ok)

	_, ok = GetValueByPath(docD, "root.lvl11.lvl3")
	require.False(t, ok)
}

func TestSetValueByPath(t *testing.T) {
	var docM any
	docM = bson.M{
		"root": bson.M{
			"lvl1": bson.M{
				"lvl2": "value",
			},
			"lvl11": "value1",
		},
	}
	newBsonM := func() any { return bson.M{} }
	var err error
	docM, err = SetValueByPath(docM, "root.lvl1.lvl2", "value2", newBsonM)
	require.NoError(t, err)

	result, ok := GetValueByPath(docM, "root.lvl1.lvl2")
	require.True(t, ok)
	require.Equal(t, result, "value2")

	docM, err = SetValueByPath(docM, "root.lvl1.lvl2", bson.M{"lvl3": "value3"}, newBsonM)
	require.NoError(t, err)

	result, ok = GetValueByPath(docM, "root.lvl1.lvl2.lvl3")
	require.True(t, ok)
	require.Equal(t, result, "value3")

	docM, err = SetValueByPath(docM, "root.lvl1.lvl2.lvl3.lvl4", "value4", newBsonM)
	require.NoError(t, err)

	result, ok = GetValueByPath(docM, "root.lvl1.lvl2.lvl3.lvl4")
	require.True(t, ok)
	require.Equal(t, result, "value4")

	docM, err = SetValueByPath(docM, "root.lvl111.lvl5", "value5", newBsonM)
	require.NoError(t, err)

	result, ok = GetValueByPath(docM, "root.lvl111.lvl5")
	require.True(t, ok)
	require.Equal(t, result, "value5")

	var docD any
	docD = bson.D{
		{Key: "root", Value: bson.D{
			{Key: "lvl1", Value: bson.D{{Key: "lvl2", Value: "value"}}},
			{Key: "lvl11", Value: "value1"},
		}},
	}
	newBsonD := func() any { return bson.D{} }
	docD, err = SetValueByPath(docD, "root.lvl1.lvl2", "value2", newBsonD)
	require.NoError(t, err)

	result, ok = GetValueByPath(docD, "root.lvl1.lvl2")
	require.True(t, ok)
	require.Equal(t, result, "value2")

	docD, err = SetValueByPath(docD, "root.lvl1.lvl2", bson.D{{Key: "lvl3", Value: "value3"}}, newBsonD)
	require.NoError(t, err)

	result, ok = GetValueByPath(docD, "root.lvl1.lvl2.lvl3")
	require.True(t, ok)
	require.Equal(t, result, "value3")

	docD, err = SetValueByPath(docD, "root.lvl1.lvl2.lvl3.lvl4", "value4", newBsonD)
	require.NoError(t, err)

	result, ok = GetValueByPath(docD, "root.lvl1.lvl2.lvl3.lvl4")
	require.True(t, ok)
	require.Equal(t, result, "value4")

	docD, err = SetValueByPath(docD, "root.lvl111.lvl5", "value5", newBsonD)
	require.NoError(t, err)

	result, ok = GetValueByPath(docD, "root.lvl111.lvl5")
	require.True(t, ok)
	require.Equal(t, result, "value5")
}

func TestMarshalDValue(t *testing.T) {
	doc := MakeDValue(bson.D{{Key: "b", Value: 1}, {Key: "aa", Value: math.NaN()}}, false, false)
	repackedDoc, err := doc.RepackValue()
	require.NoError(t, err)
	repackRes, err := json.Marshal(repackedDoc)
	require.NoError(t, err)
	require.NotEmpty(t, repackRes)

	res, err := json.Marshal(doc)
	require.NoError(t, err)
	require.NotEmpty(t, res)
}
