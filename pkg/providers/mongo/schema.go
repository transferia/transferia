//go:build !disable_mongo_provider

package mongo

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/set"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	ID              = "_id"
	UpdatedFields   = "updatedFields"
	RemovedFields   = "removedFields"
	TruncatedArrays = "truncatedArrays"
	FullDocument    = "fullDocument"
	Document        = "document"
)

var (
	DocumentSchema = newSchemaDescription(abstract.NewTableSchema([]abstract.ColSchema{{
		ColumnName:   ID,
		DataType:     ytschema.TypeString.String(),
		PrimaryKey:   true,
		OriginalType: "mongo:bson_id",
	}, {
		ColumnName:   Document,
		DataType:     ytschema.TypeAny.String(),
		OriginalType: "mongo:bson",
	}}))

	UpdateDocumentSchema = newSchemaDescription(abstract.NewTableSchema([]abstract.ColSchema{{
		ColumnName:   ID,
		DataType:     ytschema.TypeString.String(),
		PrimaryKey:   true,
		OriginalType: "mongo:bson",
	}, {
		ColumnName:   UpdatedFields,
		DataType:     ytschema.TypeAny.String(),
		OriginalType: "mongo:bson",
	}, {
		ColumnName:   RemovedFields,
		DataType:     ytschema.TypeAny.String(),
		OriginalType: "mongo:bson",
	}, {
		ColumnName:   TruncatedArrays,
		DataType:     ytschema.TypeAny.String(),
		OriginalType: "mongo:bson",
	}, {
		ColumnName:   FullDocument,
		DataType:     ytschema.TypeAny.String(),
		OriginalType: "mongo:bson",
	}}))

	CollectionFilter = bson.D{
		// Regex for collection name to exclude system collections seems to be unavailable for some price tiers in Atlas.
		// Setting it throws "(AtlasError) can't get regex from filter doc not a regex" even with an atlas search regex.
		// The only hint that this is somehow tied to the price tier comes form a support ticket https://jira.mongodb.org/browse/NODE-4455.
		// For Atlas we will default to backend collection name filtering.
		{
			Key:   "type",
			Value: "collection",
		},
	}

	SystemDBs = []string{
		"admin",
		"config",
		"local",
		"mdb_internal",
	}
)

type SchemaDescription struct {
	Columns      *abstract.TableSchema
	ColumnsNames []string
	Indexes      map[string]int
}

func newSchemaDescription(cols *abstract.TableSchema) SchemaDescription {
	names := make([]string, len(cols.Columns()))
	idx := map[string]int{}

	for i, c := range cols.Columns() {
		names[i] = c.ColumnName
		idx[c.ColumnName] = i
	}

	return SchemaDescription{
		Columns:      cols,
		ColumnsNames: names,
		Indexes:      idx,
	}
}

func ExtractKey(id interface{}, isHomo bool) (interface{}, error) {
	if !isHomo {
		switch v := id.(type) {
		case string:
			return v, nil
		case int, int32, int64, int16, uint, uint32, uint64, uint16, float64:
			return fmt.Sprintf("%v", v), nil
		case primitive.ObjectID:
			return v.Hex(), nil
		case primitive.Binary:
			return hex.EncodeToString(v.Data), nil
		default:
			data, err := json.Marshal(v)
			if err != nil {
				return "", xerrors.Errorf("cannot marshal value as JSON: %w", err)
			}
			return string(data), nil
		}
	}
	return id, nil
}

func equalColSchema(l, r abstract.ColSchema) bool {
	return l.TableSchema == r.TableSchema && l.TableName == r.TableName && l.Path == r.Path && l.ColumnName == r.ColumnName && l.DataType == r.DataType && l.PrimaryKey == r.PrimaryKey && l.FakeKey == r.FakeKey && l.Required == r.Required && l.Expression == r.Expression && l.OriginalType == r.OriginalType
}

func IsUpdateDocumentSchema(tableSchema []abstract.ColSchema) bool {
	if len(tableSchema) != len(UpdateDocumentSchema.Columns.Columns()) {
		return false
	}
	for i := 0; i < len(UpdateDocumentSchema.Columns.Columns()); i++ {
		if !equalColSchema(tableSchema[i], UpdateDocumentSchema.Columns.Columns()[i]) {
			return false
		}
	}
	return true
}

func IsNativeMongoSchema(tableSchema []abstract.ColSchema) bool {
	if len(tableSchema) != len(DocumentSchema.Columns.Columns()) {
		return false
	}
	for i := 0; i < len(DocumentSchema.Columns.Columns()); i++ {
		if !equalColSchema(tableSchema[i], DocumentSchema.Columns.Columns()[i]) {
			return false
		}
	}
	return true
}

func GetID(columns []interface{}) interface{} {
	idx := DocumentSchema.Indexes[ID]
	return columns[idx]
}

func GetDocument(columns []interface{}) bson.D {
	idx := DocumentSchema.Indexes[Document]
	if dValue, isDValue := columns[idx].(DValue); isDValue {
		return dValue.D
	}
	return columns[idx].(bson.D)
}

func GetUpdatedDocument(columns []interface{}) bson.D {
	idx := UpdateDocumentSchema.Indexes[FullDocument]
	return columns[idx].(bson.D)
}

type UpdateDocumentChangeItem struct {
	item *abstract.ChangeItem
}

func (u *UpdateDocumentChangeItem) UpdatedFields() bson.D {
	idx := UpdateDocumentSchema.Indexes[UpdatedFields]
	return u.item.ColumnValues[idx].(bson.D)
}

func (u *UpdateDocumentChangeItem) RemovedFields() []string {
	idx := UpdateDocumentSchema.Indexes[RemovedFields]
	return u.item.ColumnValues[idx].([]string)
}

func (u *UpdateDocumentChangeItem) TruncatedArrays() []TruncatedArray {
	idx := UpdateDocumentSchema.Indexes[TruncatedArrays]
	return u.item.ColumnValues[idx].([]TruncatedArray)
}

func (u *UpdateDocumentChangeItem) HasTruncatedArrays() bool {
	return len(u.TruncatedArrays()) > 0
}

func (u *UpdateDocumentChangeItem) IsApplicablePatch() bool {
	return !u.HasTruncatedArrays() && (len(u.UpdatedFields()) > 0 || len(u.RemovedFields()) > 0)
}

func (u *UpdateDocumentChangeItem) FullDocument() bson.D {
	idx := UpdateDocumentSchema.Indexes[FullDocument]
	return u.item.ColumnValues[idx].(bson.D)
}

func (u *UpdateDocumentChangeItem) CheckDiffByKeys(checkKeys []string) map[string]any {
	changedFields := map[string]any{}

	updatedFields := u.UpdatedFields()
	for _, keyPath := range checkKeys {
		val, updated := GetValueByPath(updatedFields, keyPath)
		if updated {
			changedFields[keyPath] = val
		}
	}

	checkKeysSet := set.New[string](checkKeys...)
	for _, f := range u.RemovedFields() {
		if checkKeysSet.Contains(f) {
			changedFields[f] = nil
		}
	}
	return changedFields
}

func NewUpdateDocumentChangeItem(item *abstract.ChangeItem) (*UpdateDocumentChangeItem, error) {
	if !IsUpdateDocumentSchema(item.TableSchema.Columns()) {
		return nil, xerrors.Errorf("unexpected ChangeItem schema: %v", item.TableSchema.Columns())
	}

	_, ok := item.ColumnValues[1].(bson.D)
	if !ok {
		return nil, xerrors.Errorf("unexpected type %T for updatedFields, expected bson.D", item.ColumnValues[1])
	}
	_, ok = item.ColumnValues[2].([]string)
	if !ok {
		return nil, xerrors.Errorf("unexpected type %T for removedFields, expected []string", item.ColumnValues[2])
	}
	_, ok = item.ColumnValues[3].([]TruncatedArray)
	if !ok {
		return nil, xerrors.Errorf("unexpected type %T for truncatedArrays, expected []TruncatedArray", item.ColumnValues[3])
	}
	_, ok = item.ColumnValues[4].(bson.D)
	if !ok {
		return nil, xerrors.Errorf("unexpected type %T for fullDocument, expected bson.D", item.ColumnValues[4])
	}
	return &UpdateDocumentChangeItem{item: item}, nil
}
