package mongo

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
)

func makeDocumentFilter(id interface{}, documentKey bson.M, keysPath []string) bson.D {
	result := bson.D{{Key: "_id", Value: id}}

	for _, keyPath := range keysPath {
		if keyPath != "_id" && !strings.HasPrefix(keyPath, "_id.") {
			if value, ok := GetValueByPath(documentKey, keyPath); ok {
				result = append(result, bson.E{Key: keyPath, Value: value})
			}
		}
	}
	return result
}

// filtersByIDOnly reports whether a filter from makeDocumentFilter locates the document by its root _id alone.
func filtersByIDOnly(filter interface{}) bool {
	d, ok := filter.(bson.D)
	return ok && len(d) == 1 && d[0].Key == "_id"
}

func makeUnsetBSON(paths []string) bson.D {
	var doc bson.D
	for _, path := range paths {
		if len(path) > 0 {
			doc = append(doc, bson.E{Key: path, Value: ""})
		}
	}
	return doc
}

func makeUpdateModel(filter bson.D, set bson.D, unset []string) *mongo_driver.UpdateOneModel {
	model := &mongo_driver.UpdateOneModel{}
	model.SetUpsert(true)
	model.SetFilter(filter)
	var update bson.D
	if len(set) > 0 {
		update = append(update, bson.E{Key: "$set", Value: set})
	}
	if unsetDoc := makeUnsetBSON(unset); len(unsetDoc) > 0 {
		update = append(update, bson.E{Key: "$unset", Value: unsetDoc})
	}
	if len(update) > 0 {
		model.SetUpdate(update)
	}
	return model
}

func makeDeleteModel(filter bson.D) *mongo_driver.DeleteOneModel {
	return mongo_driver.NewDeleteOneModel().SetFilter(filter)
}

func makeReplaceModel(filter bson.D, document bson.D) *mongo_driver.ReplaceOneModel {
	return mongo_driver.NewReplaceOneModel().
		SetFilter(filter).
		SetReplacement(document).
		SetUpsert(true)
}

func makeInsertModel(document bson.D) *mongo_driver.InsertOneModel {
	return mongo_driver.NewInsertOneModel().SetDocument(document)
}

// insertToUpsertFallback turns an insert model into the replace-upsert of the same document by its root _id; nil for
// other models or documents without a root _id. Inserts are used only where _id alone locates the document (see
// makeWriteModel), so the filter needs no shard key.
func insertToUpsertFallback(model mongo_driver.WriteModel) mongo_driver.WriteModel {
	insert, ok := model.(*mongo_driver.InsertOneModel)
	if !ok {
		return nil
	}
	document, ok := insert.Document.(bson.D)
	if !ok {
		return nil
	}
	id, ok := GetValueByPath(document, "_id")
	if !ok {
		return nil
	}
	return makeReplaceModel(makeDocumentFilter(id, nil, nil), document)
}
