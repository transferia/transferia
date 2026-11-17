package mongo

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// OperationType is the type of a change stream event.
type OperationType string

const (
	OperationTypeInsert       OperationType = "insert"
	OperationTypeUpdate       OperationType = "update"
	OperationTypeReplace      OperationType = "replace"
	OperationTypeDelete       OperationType = "delete"
	OperationTypeCreate       OperationType = "create"
	OperationTypeDrop         OperationType = "drop"
	OperationTypeRename       OperationType = "rename"
	OperationTypeDropDatabase OperationType = "dropDatabase"
	OperationTypeInvalidate   OperationType = "invalidate"
	OperationTypeNoop         OperationType = "noop" // OperationTypeNoop is made by the oplog parser only.
)

type KeyChangeEvent struct {
	OperationType OperationType       `bson:"operationType"`
	DocumentKey   DocumentKey         `bson:"documentKey"`
	Namespace     Namespace           `bson:"ns"`
	ToNamespace   Namespace           `bson:"to"`
	ClusterTime   primitive.Timestamp `bson:"clusterTime"`
}

func (k *KeyChangeEvent) ToChangeEvent() *ChangeEvent {
	if k == nil {
		return nil
	}
	return &ChangeEvent{
		KeyChangeEvent:    *k,
		FullDocument:      nil,
		UpdateDescription: nil,
	}
}

type ChangeEvent struct {
	KeyChangeEvent    `bson:",inline"`
	FullDocument      bson.D             `bson:"fullDocument"`
	UpdateDescription *UpdateDescription `bson:"updateDescription"`
}

type UpdateDescription struct {
	UpdatedFields   bson.D           `bson:"updatedFields"`
	RemovedFields   []string         `bson:"removedFields"`
	TruncatedArrays []TruncatedArray `bson:"truncatedArrays"`
	// DisambiguatedPaths maps the changed paths which a dotted string does not tell apart to their components.
	// MongoDB 6.1+, only with showExpandedEvents.
	DisambiguatedPaths bson.D `bson:"disambiguatedPaths"`
}

// HasDottedFieldName reports whether a changed path goes through a field with a dot in its name. Update operators
// read such a path as nested fields, so the description cannot be applied as a patch.
func (d *UpdateDescription) HasDottedFieldName() bool {
	for _, path := range d.DisambiguatedPaths {
		components, _ := path.Value.(bson.A)
		for _, component := range components {
			if name, ok := component.(string); ok && strings.Contains(name, ".") {
				return true
			}
		}
	}
	return false
}

type TruncatedArray struct {
	Field   string `bson:"field"`
	NewSize int    `bson:"newSize"`
}

type Namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

func (namespace *Namespace) GetFullName() string {
	return fmt.Sprintf("%v.%v", namespace.Database, namespace.Collection)
}

func MakeNamespace(database, collection string) Namespace {
	return Namespace{
		Database:   database,
		Collection: collection,
	}
}

func ParseNamespace(rawNamespace string) *Namespace {
	result := NewMongoCollection(rawNamespace)
	if result == nil {
		return nil
	}
	return &Namespace{
		Database:   result.DatabaseName,
		Collection: result.CollectionName,
	}
}

type DocumentKey struct {
	ID interface{} `bson:"_id"`
}
