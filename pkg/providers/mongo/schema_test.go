//go:build !disable_mongo_provider

package mongo

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
)

func TestRepack(t *testing.T) {
	t.Run("no-homo", func(t *testing.T) {
		res, err := repack(bson.D{{Key: "a", Value: 123}, {Key: "b", Value: bson.D{
			{Key: "nan", Value: math.NaN()}, {Key: "str", Value: "str"},
		}}}, false, false)
		require.NoError(t, err)
		data, err := json.Marshal(res)
		require.NoError(t, err)
		logger.Log.Infof("data: %s", string(data))
	})
	t.Run("no-homo without repack", func(t *testing.T) {
		res, err := repack(bson.D{{Key: "a", Value: 123}, {Key: "b", Value: bson.D{
			{Key: "b", Value: "asd"}, {Key: "str", Value: "str"},
		}}}, false, true)
		require.NoError(t, err)
		data, err := json.Marshal(res)
		require.NoError(t, err)
		logger.Log.Infof("data: %s", string(data))
	})
	t.Run("no-homo without repack but very bad no-homo", func(t *testing.T) {
		res, err := repack(bson.D{{Key: "a", Value: 123}, {Key: "b", Value: bson.D{
			{Key: "b", Value: math.NaN()}, {Key: "str", Value: "str"},
		}}}, false, true)
		require.NoError(t, err)
		_, err = json.Marshal(res)
		require.Error(t, err)
	})
	t.Run("homo", func(t *testing.T) {
		res, err := repack(bson.D{{Key: "a", Value: 123}, {Key: "b", Value: math.NaN()}}, true, false)
		require.NoError(t, err)
		_, err = json.Marshal(res)
		require.Error(t, err)
	})
	t.Run("homo but good homo", func(t *testing.T) {
		res, err := repack(bson.D{{Key: "a", Value: 123}, {Key: "b", Value: "asd"}}, true, false)
		require.NoError(t, err)
		data, err := json.Marshal(res)
		require.NoError(t, err)
		logger.Log.Infof("data: %s", string(data))
	})
	t.Run("homo doesn't give a shit about repacking", func(t *testing.T) {
		doc := bson.D{{Key: "a", Value: 123}, {Key: "b", Value: "asd"}}
		res1, err := repack(doc, true, false)
		require.NoError(t, err)
		res2, err := repack(doc, true, false)
		require.NoError(t, err)
		require.Equal(t, res1, res2)
	})
}

func TestUpdateDocumentChangeItem(t *testing.T) {
	changeItem := abstract.ChangeItem{
		ID:         0,
		LSN:        0,
		CommitTime: 0,
		Counter:    0,
		Kind:       abstract.MongoUpdateDocumentKind,
		Schema:     "db_user",
		Table:      "coll",
		ColumnNames: []string{
			"_id",
			"updatedFields",
			"removedFields",
			"truncatedArrays",
			"fullDocument",
		},
		ColumnValues: []interface{}{
			"iddtt",
			bson.D{bson.E{Key: "document", Value: bson.D{bson.E{Key: "a", Value: "nrg4g5dp"}, bson.E{Key: "b", Value: "pfvwipvl"}, bson.E{Key: "c", Value: "zzdg8e5k"}, bson.E{Key: "x", Value: "ztpg5tnv"}, bson.E{Key: "y", Value: "2yv4kv8h"}, bson.E{Key: "z", Value: "u4rzozx0"}}}},
			[]string{"document.e"},
			[]TruncatedArray{},
			bson.D{},
		},
		TableSchema: UpdateDocumentSchema.Columns,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{"_id"},
			KeyTypes:  nil,
			KeyValues: []interface{}{"iddtt"},
		},
		TxID:  "",
		Query: "",
		Size:  abstract.RawEventSize(0),
	}
	updateItem, err := NewUpdateDocumentChangeItem(&changeItem)
	require.NoError(t, err)
	require.True(t, updateItem.IsApplicablePatch())
	patch := updateItem.CheckDiffByKeys([]string{"document.a", "document.b", "document.c"})
	require.Equal(t, len(patch), 3)
	require.Equal(t, patch["document.a"], "nrg4g5dp")
	require.Equal(t, patch["document.b"], "pfvwipvl")
	require.Equal(t, patch["document.e"], nil)
}

func TestSchemasDescriptions(t *testing.T) {
	_, ok := DocumentSchema.Indexes[ID]
	require.True(t, ok)
	_, ok = DocumentSchema.Indexes[Document]
	require.True(t, ok)

	_, ok = UpdateDocumentSchema.Indexes[ID]
	require.True(t, ok)
	_, ok = UpdateDocumentSchema.Indexes[UpdatedFields]
	require.True(t, ok)
	_, ok = UpdateDocumentSchema.Indexes[RemovedFields]
	require.True(t, ok)
	_, ok = UpdateDocumentSchema.Indexes[TruncatedArrays]
	require.True(t, ok)
	_, ok = UpdateDocumentSchema.Indexes[FullDocument]
	require.True(t, ok)
}

func repack(val bson.D, isHomo, preventJSONRepack bool) (interface{}, error) {
	dValue := MakeDValue(val, isHomo, preventJSONRepack)
	return dValue.RepackValue()
}
