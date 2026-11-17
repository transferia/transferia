package mongo

import (
	"sort"
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

// patchStep is one update of a document patch; push truncates arrays.
type patchStep struct {
	push, set, unset bson.D
}

// makeUpdateModels turns the update description of a change stream event into update operators: updatedFields
// into $set, removedFields into $unset, truncatedArrays into {$push: {<field>: {$each: [], $slice: <newSize>}}}.
// The server rejects an update that truncates an array and writes below it at once, so a write below a truncated
// array goes to the update after the one truncating it; the updates must be applied in the returned order. The
// models do not upsert: a patch of a missing document would create a stub out of the changed fields. Nothing is
// returned for an empty description.
func makeUpdateModels(
	filter bson.D,
	set bson.D,
	unset []string,
	truncated []TruncatedArray,
) []mongo_driver.WriteModel {
	var steps []patchStep
	// stepOf returns the first step which may write to the path: the one after the last truncating an array above.
	stepOf := func(path string) *patchStep {
		at := 0
		for i := range steps {
			for _, array := range steps[i].push {
				if path == array.Key || strings.HasPrefix(path, array.Key+".") {
					at = i + 1
				}
			}
		}
		for len(steps) <= at {
			steps = append(steps, patchStep{push: nil, set: nil, unset: nil})
		}
		return &steps[at]
	}

	arrays := append([]TruncatedArray(nil), truncated...)
	sort.SliceStable(arrays, func(i, j int) bool { return len(arrays[i].Field) < len(arrays[j].Field) }) // outer first
	for _, array := range arrays {
		step := stepOf(array.Field)
		step.push = append(step.push, bson.E{Key: array.Field, Value: bson.D{
			{Key: "$each", Value: bson.A{}},
			{Key: "$slice", Value: array.NewSize},
		}})
	}
	for _, field := range set {
		step := stepOf(field.Key)
		step.set = append(step.set, field)
	}
	for _, path := range unset {
		if len(path) > 0 {
			step := stepOf(path)
			step.unset = append(step.unset, bson.E{Key: path, Value: ""})
		}
	}

	models := make([]mongo_driver.WriteModel, 0, len(steps))
	for _, step := range steps {
		var update bson.D
		if len(step.push) > 0 {
			update = append(update, bson.E{Key: "$push", Value: step.push})
		}
		if len(step.set) > 0 {
			update = append(update, bson.E{Key: "$set", Value: step.set})
		}
		if len(step.unset) > 0 {
			update = append(update, bson.E{Key: "$unset", Value: step.unset})
		}
		if len(update) > 0 {
			models = append(models, mongo_driver.NewUpdateOneModel().SetFilter(filter).SetUpdate(update))
		}
	}
	return models
}

// isPatchModel reports whether the model is an update made by makeUpdateModels; the sink makes no other updates.
func isPatchModel(model mongo_driver.WriteModel) bool {
	_, ok := model.(*mongo_driver.UpdateOneModel)
	return ok
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
// makeWriteModels), so the filter needs no shard key.
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
