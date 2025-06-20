//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/set"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

type shardedCollectionSinkContext struct {
	client     *MongoClientWrapper
	collection Namespace
	shardKey   *ShardKeysInfo

	docKeysCache map[string]bson.M
	enabled      bool
}

func (c *shardedCollectionSinkContext) Init(ctx context.Context, documentIDs []documentID) error {
	if !c.enabled || c.IsTrivial() {
		return xerrors.Errorf("sharding for %v is not enabled or trivial(sharded by _id)", c.collection.GetFullName())
	}

	idSet := set.New[string]()
	rawIDs := []interface{}{}
	for _, id := range documentIDs {
		if !idSet.Contains(id.String) {
			idSet.Add(id.String)
			rawIDs = append(rawIDs, id.Raw)
		}
	}

	filter := bson.M{"_id": bson.M{"$in": rawIDs}}
	projection := keysProjection(c.shardKey.Fields())

	coll := c.client.Database(c.collection.Database).Collection(c.collection.Collection)
	cur, err := coll.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return xerrors.Errorf("cannot search for documents keys from %v: %w", c.collection.GetFullName(), err)
	}

	var result []bson.M
	err = cur.All(ctx, &result)
	if err != nil {
		return xerrors.Errorf("cannot read search result for documents keys from %v: %w", c.collection.GetFullName(), err)
	}

	c.docKeysCache = map[string]bson.M{}
	for _, key := range result {
		c.docKeysCache[fmt.Sprintf("%#v", key["_id"])] = key
	}
	return nil
}

func keysProjection(keys []string) bson.D {
	projection := bson.D{{Key: "_id", Value: 1}}
	for _, key := range keys {
		if key != "_id" && !strings.HasPrefix(key, "_id.") {
			projection = append(projection, bson.E{Key: key, Value: 1})
		}
	}
	return projection
}

func (c *shardedCollectionSinkContext) IsTrivial() bool {
	return c.enabled && c.shardKey.IsTrivial()
}

func (c *shardedCollectionSinkContext) Enabled() bool {
	return c.enabled
}

func (c *shardedCollectionSinkContext) KeyFields() []string {
	if !c.Enabled() {
		return nil
	}
	return c.shardKey.Fields()
}

func (c *shardedCollectionSinkContext) ContainsID() bool {
	if !c.Enabled() {
		return false
	}
	return c.shardKey.ContainsID()
}

func (c *shardedCollectionSinkContext) GetDocumentKey(id documentID, item *abstract.ChangeItem) (storedKey bson.M, updateKey bool, err error) {
	storedKey = c.docKeysCache[id.String]

	if item.Kind == abstract.DeleteKind {
		if storedKey != nil {
			delete(c.docKeysCache, id.String)
		}
		return storedKey, false, nil
	}

	var resultDocKey bson.M
	resultDocKey, updateKey, err = getResultDocumentKey(item, storedKey, c.shardKey.Fields())
	if err != nil {
		return nil, false, xerrors.Errorf("cannot infer document(with _id=%v) key from collection %v according to change event: %w", id.String, c.collection.GetFullName(), err)
	}

	if updateKey || storedKey == nil {
		c.docKeysCache[id.String] = resultDocKey
	}

	if storedKey != nil {
		return storedKey, updateKey, nil
	}

	return resultDocKey, false, nil
}

func getResultDocumentKey(item *abstract.ChangeItem, storedDocKey bson.M, shardKeyFields []string) (resultKey bson.M, keyChanged bool, err error) {
	itemFullDocKey, err := getItemDocumentKey(item, shardKeyFields)
	if err != nil {
		return nil, false, xerrors.Errorf("cannot get document _id from ChangeItem: %w", err)
	}

	resultKey = itemFullDocKey

	if storedDocKey == nil {
		return resultKey, false, nil
	}
	if item.Kind == abstract.MongoUpdateDocumentKind {
		updateItem, err := NewUpdateDocumentChangeItem(item)
		if err != nil {
			return nil, false, xerrors.Errorf("invalid ChangeItem for MongoUpdateDocumentKind: %w", err)
		}
		if updateItem.IsApplicablePatch() { // otherwise full document will be used
			keyPatch := updateItem.CheckDiffByKeys(shardKeyFields)
			resultKey, err = updateDocument(storedDocKey, keyPatch)
			if err != nil {
				return nil, false, xerrors.Errorf("cannot define result document key after operation: %w", err)
			}
		}
	}
	keyChanged = hasDiff(storedDocKey, resultKey, shardKeyFields)
	return resultKey, keyChanged, nil
}

func newShardedCollectionSinkContext(ctx context.Context, client *MongoClientWrapper, collection Namespace, lgr log.Logger) *shardedCollectionSinkContext {
	shardKey, err := GetShardingKey(ctx, client, collection)
	if err != nil {
		lgr.Debugf("cannot get sharding key for %v: %v", collection.GetFullName(), err)
	}
	return &shardedCollectionSinkContext{
		client:       client,
		collection:   collection,
		shardKey:     shardKey,
		enabled:      shardKey != nil,
		docKeysCache: map[string]bson.M{},
	}
}
