//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

var keyExistsFilter = bson.E{
	Key: "key",
	Value: bson.D{{
		Key:   "$exists",
		Value: true,
	}},
}

type ShardKeysInfo struct {
	ID           string              `bson:"_id"`
	Lastmod      primitive.DateTime  `bson:"lastmod"`
	Timestamp    primitive.Timestamp `bson:"timestamp"`
	Key          bson.D              `bson:"key"`
	Unique       bool                `bson:"unique"`
	LastmodEpoch primitive.ObjectID  `bson:"lastmodEpoch"`
	// UUID issue: https://stackoverflow.com/questions/64723089/how-to-store-a-uuid-in-mongodb-with-golang
	// UUID         interface{}          `bson:"uuid"`
	KeyFields  []string
	isTrivial  bool
	containsID bool
}

func (s *ShardKeysInfo) GetNamespace() *Namespace {
	return ParseNamespace(s.ID)
}

func (s *ShardKeysInfo) Fields() []string {
	if len(s.KeyFields) != len(s.Key) {
		s.KeyFields = make([]string, len(s.Key))
		for i, keyItem := range s.Key {
			s.KeyFields[i] = keyItem.Key
		}
	}
	return s.KeyFields
}

func (s *ShardKeysInfo) IsTrivial() bool {
	return s.isTrivial
}

func (s *ShardKeysInfo) ContainsID() bool {
	return s.containsID
}

func isTrivialKey(key bson.D) bool {
	for _, keyItem := range key {
		if !useID(keyItem.Key) {
			return false
		}
	}
	return true
}

func containsID(key bson.D) bool {
	for _, keyItem := range key {
		if useID(keyItem.Key) {
			return true
		}
	}
	return false
}

func useID(keyName string) bool {
	return keyName == "_id" || strings.HasPrefix(keyName, "_id.")
}

func GetShardingKey(ctx context.Context, client *MongoClientWrapper, ns Namespace) (*ShardKeysInfo, error) {
	collections := client.Database("config").Collection("collections")
	var result ShardKeysInfo
	err := collections.FindOne(ctx, bson.D{
		{Key: "_id", Value: ns.GetFullName()},
		keyExistsFilter,
	}).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to find document with _id='%v' in config.collections: %w",
			ns.GetFullName(), err)
	}
	result.isTrivial = isTrivialKey(result.Key)
	result.containsID = containsID(result.Key)
	return &result, nil
}
