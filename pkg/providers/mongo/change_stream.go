//go:build !disable_mongo_provider

package mongo

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type KeyChangeEvent struct {
	OperationType string              `bson:"operationType"`
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
