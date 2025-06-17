//go:build !disable_mongo_provider

package mongo

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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

func makeUpdateModel(filter bson.D, set bson.D, unset []string) *mongo.UpdateOneModel {
	model := &mongo.UpdateOneModel{}
	model.SetUpsert(true)
	model.SetFilter(filter)
	if len(set) > 0 {
		model.SetUpdate(bson.D{{Key: "$set", Value: set}})
	}
	if len(unset) > 0 {
		model.SetUpdate(bson.D{{Key: "$unset", Value: unset}})
	}
	return model
}

func makeDeleteModel(filter bson.D) *mongo.DeleteOneModel {
	return mongo.NewDeleteOneModel().SetFilter(filter)
}

func makeReplaceModel(filter bson.D, document bson.D) *mongo.ReplaceOneModel {
	return mongo.NewReplaceOneModel().
		SetFilter(filter).
		SetReplacement(document).
		SetUpsert(true)
}
