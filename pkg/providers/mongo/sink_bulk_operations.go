package mongo

import (
	"context"
	"hash/fnv"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"go.mongodb.org/mongo-driver/bson"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
)

const sinkWriteConcurrency = 16 // sinkWriteConcurrency is maximum number of shards flushed concurrently per collection.

// splitItemsToBulkOperations distributes items into slice of bulks by hashing the _id, so every row for a given
// document will always be in the same bulk and keeps its event order. An ordered list of bulks is returned.
func (s *sinker) splitItemsToBulkOperations(ctx context.Context, collID Namespace, items []abstract.ChangeItem) ([][][]mongo_driver.WriteModel, error) {
	startPrepare := time.Now()

	docIDs, err := extractDocumentIDs(items)
	if err != nil {
		return nil, xerrors.Errorf("cannot extract documents _id from change items for %v: %w", collID.GetFullName(), err)
	}

	sharding := newShardedCollectionSinkContext(ctx, s.client, collID, s.logger)

	if sharding.Enabled() && !sharding.IsTrivial() {
		if err := sharding.Init(ctx, docIDs); err != nil {
			return nil, xerrors.Errorf("cannot init context for sharded collection %v: %w", collID.GetFullName(), err)
		}
	}

	splitters := make([]bulkSplitter, sinkWriteConcurrency)
	for i := range splitters {
		splitters[i] = newBulkSplitter()
	}
	for i, item := range items {
		docID := docIDs[i]

		var docKey bson.M
		shouldBeIsolated := false

		if sharding.Enabled() && !sharding.IsTrivial() {
			var err error
			docKey, shouldBeIsolated, err = sharding.GetDocumentKey(docID, &item)
			if err != nil {
				return nil, xerrors.Errorf("cannot get document(with _id=%v) key from sharded collection %v: %w", docID.String, collID.GetFullName(), err)
			}
		}

		writeModel, err := s.makeWriteModel(&item, docID, docKey, sharding)
		if err != nil {
			return nil, xerrors.Errorf("cannot prepare item to write into collection %v: %w", collID.GetFullName(), err)
		}

		splitters[shardOfDocumentID(docID.String, len(splitters))].Add(writeModel, docID, shouldBeIsolated)
	}
	result := make([][][]mongo_driver.WriteModel, 0, len(splitters))
	totalBulks := 0
	for i := range splitters {
		if bulks := splitters[i].Get(); len(bulks) > 0 {
			result = append(result, bulks)
			totalBulks += len(bulks)
		}
	}

	prepareDuration := time.Since(startPrepare)
	s.metrics.RecordDuration("bulkPrepare", prepareDuration)
	s.logger.Debugf(
		"%v: %d items prepared into %d shard(s), %d bulk(s) in %v",
		collID.GetFullName(), len(items), len(result), totalBulks, prepareDuration)
	return result, nil
}

// shardOfDocumentID maps a document _id (string form) to a write shard index in [0, shards).
func shardOfDocumentID(id string, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(shards))
}

func extractDocumentIDs(items []abstract.ChangeItem) ([]documentID, error) {
	allDocumentIDs := make([]documentID, len(items))
	for i, item := range items {
		id, err := getDocumentID(&item)
		if err != nil {
			return nil, err
		}
		allDocumentIDs[i] = id
	}
	return allDocumentIDs, nil
}

// bulkWrite writes one bulk. All operations target distinct _ids (guaranteed by bulkSplitter), so Ordered(false) is safe – MongoDB can apply them in any order.
func (s *sinker) bulkWrite(ctx context.Context, collID Namespace, bulk []mongo_driver.WriteModel) error {
	coll := s.client.Database(collID.Database).Collection(collID.Collection)
	opts := mongo_options.BulkWrite().SetOrdered(false)

	docsNumber := len(bulk)
	startWrite := time.Now()
	result, err := coll.BulkWrite(ctx, bulk, opts)
	if err != nil {
		if strings.Contains(err.Error(), "is too large") {
			s.logger.Warnf("BulkWrite(%v documents) to %v failed: %v. Try serial push instead.", docsNumber, collID.GetFullName(), err)

			serialPushResult, serialPushErr := s.serialPush(ctx, collID, bulk)
			if serialPushErr != nil {
				return xerrors.Errorf("cannot write %v documents to %v neither by BulkWrite(%v) nor by serial push: %w", docsNumber, collID.GetFullName(), err, serialPushErr)
			}
			result = serialPushResult
		} else if strings.Contains(err.Error(), "could not extract exact shard key") {
			return xerrors.Errorf("cannot write %v documents to sharded collection %v - check if user has clusterManager or mdbShardingManager role: %w", docsNumber, collID.GetFullName(), err)
		} else if strings.Contains(err.Error(), "BSONObjectTooLarge") || strings.Contains(err.Error(), "Tried to create string longer than 16MB") {
			// Fallback by message
			if strings.Contains(err.Error(), "Tried to create string longer than 16MB") {
				return coded.Errorf(error_codes.MongoCollectionKeyTooLarge, "bulk write failed for %v: %w", collID.GetFullName(), err)
			}
			return coded.Errorf(error_codes.MongoBSONObjectTooLarge, "bulk write failed for %v: %w", collID.GetFullName(), err)
		} else {
			return xerrors.Errorf("bulk write of %v documents to %v failed: %w", docsNumber, collID.GetFullName(), err)
		}
	}
	s.setBulkWriteMetrics(collID, time.Since(startWrite), docsNumber, result)
	return nil
}

// serialPush writes models one by one.
func (s *sinker) serialPush(ctx context.Context, collID Namespace, bulk []mongo_driver.WriteModel) (*mongo_driver.BulkWriteResult, error) {
	coll := s.client.Database(collID.Database).Collection(collID.Collection)
	opts := mongo_options.BulkWrite().SetOrdered(false)
	totalResult := mongo_driver.BulkWriteResult{}
	for i, m := range bulk {
		iResult, iErr := coll.BulkWrite(ctx, []mongo_driver.WriteModel{m}, opts)
		if iErr != nil {
			return nil, xerrors.Errorf("failed push %v-th document: %w", i, iErr)
		}
		updateBulkWriteResult(&totalResult, *iResult)
	}
	return &totalResult, nil
}

func updateBulkWriteResult(totalResult *mongo_driver.BulkWriteResult, partialResult mongo_driver.BulkWriteResult) {
	totalResult.InsertedCount += partialResult.InsertedCount
	totalResult.MatchedCount += partialResult.MatchedCount
	totalResult.ModifiedCount += partialResult.ModifiedCount
	totalResult.DeletedCount += partialResult.DeletedCount
	totalResult.UpsertedCount += partialResult.UpsertedCount
}

func (s *sinker) setBulkWriteMetrics(collID Namespace, duration time.Duration, docsCount int, result *mongo_driver.BulkWriteResult) {
	s.metrics.RecordDuration("bulkWrite", duration)
	s.metrics.Table(collID.Collection, "rows", docsCount)
	s.metrics.Table(collID.Collection, "deleted_rows", int(result.DeletedCount))
	s.metrics.Table(collID.Collection, "upserted_rows", int(result.UpsertedCount))
	s.metrics.Table(collID.Collection, "updated_rows", int(result.MatchedCount))
	s.logger.Debugf(
		"%v.%v: %d documents written in %v: upserted(%d), updated(%d), deleted(%d)",
		collID.Database, collID.Collection, docsCount, duration,
		result.UpsertedCount, result.MatchedCount, result.DeletedCount)
}

func (s *sinker) makeWriteModel(chgItem *abstract.ChangeItem, id documentID, docKey bson.M, shardKey *shardedCollectionSinkContext) (mongo_driver.WriteModel, error) {
	switch chgItem.Kind {
	case abstract.InsertKind, abstract.UpdateKind:
		filter := makeDocumentFilter(id.Raw, docKey, shardKey.KeyFields())
		document, err := getDocument(chgItem, shardKey)
		if err != nil {
			return nil, xerrors.Errorf("cannot compose MongoDB document from ChangeItem: %w", err)
		}
		return makeReplaceModel(filter, document), nil
	case abstract.DeleteKind:
		filter := makeDocumentFilter(id.Raw, nil, nil)
		return makeDeleteModel(filter), nil
	case abstract.MongoUpdateDocumentKind:
		updateItem, err := NewUpdateDocumentChangeItem(chgItem)
		if err != nil {
			return nil, abstract.NewFatalError(xerrors.Errorf("ivalid ChangeItem for MongoUpdateDocumentKind: %w", err))
		}
		filter := makeDocumentFilter(id.Raw, docKey, shardKey.KeyFields())

		if !updateItem.IsApplicablePatch() {
			fullDocument := updateItem.FullDocument()
			return makeReplaceModel(filter, fullDocument), nil
		} else {
			updatedFields := updateItem.UpdatedFields()
			removedFields := updateItem.RemovedFields()
			return makeUpdateModel(filter, updatedFields, removedFields), nil
		}
	default:
		return nil, makeErrOperationKindNotSupported(chgItem)
	}
}
