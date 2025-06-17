//go:build !disable_mongo_provider

package mongo

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
)

func getDocument(chgItem *abstract.ChangeItem, shardKey *shardedCollectionSinkContext) (bson.D, error) {
	var document bson.D
	var err error
	if IsUpdateDocumentSchema(chgItem.TableSchema.Columns()) {
		document = GetUpdatedDocument(chgItem.ColumnValues)
	} else if IsNativeMongoSchema(chgItem.TableSchema.Columns()) {
		document = GetDocument(chgItem.ColumnValues)
	} else { // heterogeneous transfer
		document, err = buildBsonDValues(chgItem)
		if err != nil {
			return nil, xerrors.Errorf("cannot build document bson.D from input event item: %w", err)
		}
	}

	if shardKey != nil && shardKey.Enabled() && shardKey.ContainsID() && !containsInRoot(document, "_id") {
		// https://www.mongodb.com/docs/manual/reference/method/db.collection.replaceOne/#sharded-collections
		// In earlier versions(<4.2), the operation attempts to target using the replacement document.
		documentID, err := getDocumentID(chgItem)
		if err != nil {
			return nil, xerrors.Errorf("cannot extract document _id from input event item: %w", err)
		}
		document = append(document, bson.E{Key: "_id", Value: documentID.Raw})
	}
	return document, nil
}

func containsInRoot(document bson.D, keyName string) bool {
	for _, rootItem := range document {
		if rootItem.Key == keyName {
			return true
		}
	}
	return false
}

func buildBsonDValues(chgItem *abstract.ChangeItem) (bson.D, error) {
	document := make(bson.D, 0, len(chgItem.ColumnValues))
	for i := 0; i < len(chgItem.ColumnValues); i++ {
		value, err := ValueInMongoFormat(chgItem, i)
		if err != nil {
			return nil, xerrors.Errorf("failed to retrieve value %d in MongoDB format: %w", i, err)
		}
		document = append(document, bson.E{
			Key:   chgItem.ColumnNames[i],
			Value: value,
		})
	}
	return document, nil
}

type documentID struct {
	Raw    interface{}
	String string
}

func getDocumentID(chgItem *abstract.ChangeItem) (documentID, error) {
	id := new(documentID)
	if IsUpdateDocumentSchema(chgItem.TableSchema.Columns()) || IsNativeMongoSchema(chgItem.TableSchema.Columns()) {
		id.Raw = getNativeDocumentID(chgItem)
	} else {
		// heterogeneous transfer
		if generatedID, err := makeDocumentID(chgItem); err != nil {
			return *id, err
		} else {
			id.Raw = generatedID
		}
	}
	id.String = fmt.Sprintf("%#v", id.Raw)
	return *id, nil
}

func getNativeDocumentID(chgItem *abstract.ChangeItem) interface{} {
	if chgItem.Kind == abstract.InsertKind {
		return GetID(chgItem.ColumnValues)
	}
	return GetID(chgItem.OldKeys.KeyValues)
}

func makeDocumentID(chgItem *abstract.ChangeItem) (interface{}, error) {
	var keyVals []string
	if chgItem.Kind == abstract.InsertKind {
		keyVals = chgItem.KeyVals()
	} else {
		keyVals = make([]string, 0, len(chgItem.OldKeys.KeyValues))
		for v := range chgItem.OldKeys.KeyValues {
			keyVals = append(keyVals, fmt.Sprintf("%v", v))
		}
	}
	if len(keyVals) == 0 {
		return nil, xerrors.Errorf("No primary keys found for change item: %s", util.Sample(chgItem.ToJSONString(), 10000))
	}
	return encodeID(keyVals), nil
}

func encodeID(keyVals []string) (result string) {
	replacer := strings.NewReplacer("\\", "\\\\", "-", "\\-")
	if len(keyVals) > 0 {
		result += replacer.Replace(keyVals[0])
	}
	for i := 1; i < len(keyVals); i++ {
		result += "-"
		result += replacer.Replace(keyVals[i])
	}
	return result
}

func getItemDocumentKey(chgItem *abstract.ChangeItem, keyFields []string) (bson.M, error) {
	bsonDocument, err := getDocument(chgItem, nil)
	if err != nil {
		return nil, xerrors.Errorf("invalid change event: cannot extract document: %w", err)
	}
	doc := bsonDocument.Map()
	key := bson.M{}
	for _, field := range keyFields {
		if field == "_id" {
			docID, err := getDocumentID(chgItem)
			if err != nil {
				return nil, xerrors.Errorf("cannot extract _id of document: %w", err)
			}
			key["_id"] = docID.Raw
		} else {
			fieldValue, _ := GetValueByPath(doc, field)
			if _, err = SetValueByPath(key, field, fieldValue, func() any { return bson.M{} }); err != nil {
				return nil, xerrors.Errorf("cannot set key %v value: %w", field, err)
			}
		}
	}
	return key, nil
}

func hasDiff(left, right bson.M, keys []string) bool {
	for _, key := range keys {
		lValue, lOk := GetValueByPath(left, key)
		rValue, rOk := GetValueByPath(right, key)
		if lOk && rOk && !reflect.DeepEqual(lValue, rValue) {
			return true
		}
		if (!lOk || !rOk) && (lValue != nil || rValue != nil) {
			return true
		}
	}
	return false
}

func updateDocument(doc bson.M, patch map[string]any) (bson.M, error) {
	dup, err := copyDocument(doc)
	if err != nil {
		return nil, xerrors.Errorf("cannot make copy of base document: %w", err)
	}
	result, ok := dup.(bson.M)
	if !ok {
		return nil, xerrors.Errorf("unexpected type of copied document: %T, but bson.M was expected", dup)
	}
	for fieldPath, value := range patch {
		if _, err := SetValueByPath(result, fieldPath, value, func() any { return bson.M{} }); err != nil {
			return nil, xerrors.Errorf("cannot update key %v value: %w", fieldPath, err)
		}
	}
	return result, nil
}

func copyDocument(orig any) (any, error) {
	switch o := orig.(type) {
	case bson.M:
		if dup, err := copyBsonM(o); err != nil {
			return nil, xerrors.Errorf("cannot create a copy of bson.M: %w", err)
		} else {
			return dup, nil
		}
	case bson.D:
		if dup, err := copyBsonD(o); err != nil {
			return nil, xerrors.Errorf("cannot create a copy of bson.D: %w", err)
		} else {
			return dup, nil
		}
	case bson.E:
		if dup, err := copyBsonE(o); err != nil {
			return nil, xerrors.Errorf("cannot create a copy of bson.E: %w", err)
		} else {
			return dup, nil
		}
	default:
		dup, err := copyAny(o)
		if err != nil {
			return nil, xerrors.Errorf("cannot create a copy of %T value: %w", orig, err)
		}
		return dup, nil
	}
}

var complexKinds = map[reflect.Kind]bool{
	reflect.Array:  true,
	reflect.Map:    true,
	reflect.Slice:  true,
	reflect.String: true,
	reflect.Struct: true,
}

func copyAny(orig any) (any, error) {
	copyComplex := func(in, out any) error {
		copyManager, err := copier()
		if err != nil {
			return xerrors.Errorf("cannot copy document: %w", err)
		}
		err = copyManager.Copy(in, out)
		if err != nil {
			return xerrors.Errorf("cannot copy document: %w", err)
		}
		return nil
	}

	isComplexKind := func(k reflect.Kind) bool {
		return complexKinds[k]
	}

	if orig == nil {
		return nil, nil
	}
	origType := reflect.TypeOf(orig)
	if isComplexKind(origType.Kind()) {
		dupPtr := reflect.New(origType).Interface()
		if err := copyComplex(orig, dupPtr); err != nil {
			return nil, err
		}
		return reflect.Indirect(reflect.ValueOf(dupPtr)).Interface(), nil
	}
	return orig, nil
}

func copyBsonM(orig bson.M) (bson.M, error) {
	cp := bson.M{}
	var err error
	for key, value := range orig {
		if cp[key], err = copyDocument(value); err != nil {
			return nil, err
		}
	}
	return cp, nil
}

func copyBsonD(orig bson.D) (bson.D, error) {
	cp := make(bson.D, len(orig))
	var err error
	for i, e := range orig {
		if cp[i], err = copyBsonE(e); err != nil {
			return nil, err
		}
	}
	return cp, nil
}

func copyBsonE(orig bson.E) (bson.E, error) {
	dup, err := copyDocument(orig.Value)
	if err != nil {
		return bson.E{}, err
	}
	return bson.E{
		Key:   orig.Key,
		Value: dup,
	}, nil
}
