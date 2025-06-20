//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (s *sinker) splitItemsToBulkOperations(ctx context.Context, collID Namespace, items []abstract.ChangeItem) ([][]mongo.WriteModel, error) {
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

	bulks := newBulkSplitter()
	for i, item := range items {
		docID := docIDs[i]

		var docKey bson.M
		shouldBeIsolated := false

		if sharding.Enabled() && !sharding.IsTrivial() {
			docKey, shouldBeIsolated, err = sharding.GetDocumentKey(docID, &item)
			if err != nil {
				return nil, xerrors.Errorf("cannot get document(with _id=%v) key from sharded collection %v: %w", docID.String, collID.GetFullName(), err)
			}
		}

		writeModel, err := s.makeWriteModel(&item, docID, docKey, sharding)
		if err != nil {
			return nil, xerrors.Errorf("cannot prepare item to write into collection %v: %w", collID.GetFullName(), err)
		}

		bulks.Add(writeModel, docID, shouldBeIsolated)
	}

	prepareDuration := time.Since(startPrepare)
	s.metrics.RecordDuration("bulkPrepare", prepareDuration)
	s.logger.Debugf(
		"%v: %d documents prepared in %v",
		collID.GetFullName(), len(items), prepareDuration)
	return bulks.Get(), nil
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

func (s *sinker) bulkWrite(ctx context.Context, collID Namespace, bulk []mongo.WriteModel) error {
	coll := s.client.Database(collID.Database).Collection(collID.Collection)
	opts := options.BulkWrite().SetOrdered(false)

	serialPush := func() (mongo.BulkWriteResult, error) {
		totalResult := mongo.BulkWriteResult{}
		for i, m := range bulk {
			iResult, iErr := coll.BulkWrite(ctx, []mongo.WriteModel{m}, opts)
			if iErr != nil {
				return totalResult, xerrors.Errorf("failed push %v-th document: %w", i, iErr)
			}
			updateBulkWriteResult(&totalResult, *iResult)
		}
		return totalResult, nil
	}

	docsNumber := len(bulk)
	startWrite := time.Now()
	result, err := coll.BulkWrite(ctx, bulk, opts)
	if err != nil {
		if strings.Contains(err.Error(), "is too large") {
			s.logger.Warnf("BulkWrite(%v documents) to %v failed: %v. Try serial push instead.", docsNumber, collID.GetFullName(), err)

			serialPushResult, serialPushErr := serialPush()
			if serialPushErr != nil {
				return xerrors.Errorf("cannot write %v documents niether by BulkWrite(%v) nor by serial push: %w", docsNumber, err, serialPushErr)
			}
			result = &serialPushResult
		} else if strings.Contains(err.Error(), "could not extract exact shard key") {
			return xerrors.Errorf("cannot write %v documents to sharded collection %v - check if user has clusterManager or mdbShardingManager role: %w", docsNumber, collID.GetFullName(), err)
		} else {
			return err
		}
	}
	s.setBulkWriteMetrics(collID, time.Since(startWrite), docsNumber, result)
	return nil
}

func updateBulkWriteResult(totalResult *mongo.BulkWriteResult, partialResult mongo.BulkWriteResult) {
	totalResult.InsertedCount += partialResult.InsertedCount
	totalResult.MatchedCount += partialResult.MatchedCount
	totalResult.ModifiedCount += partialResult.ModifiedCount
	totalResult.DeletedCount += partialResult.DeletedCount
	totalResult.UpsertedCount += partialResult.UpsertedCount
}

func (s *sinker) setBulkWriteMetrics(collID Namespace, duration time.Duration, docsCount int, result *mongo.BulkWriteResult) {
	s.metrics.RecordDuration("bulkWrite", duration)
	s.metrics.Table(collID.Collection, "rows", docsCount)
	s.metrics.Table(collID.Collection, "deleted_rows", int(result.DeletedCount))
	s.metrics.Table(collID.Collection, "upserted_rows", int(result.UpsertedCount))
	s.metrics.Table(collID.Collection, "updated_rows", int(result.MatchedCount))
	s.logger.Infof(
		"%v.%v: %d documents written in %v: upserted(%d), updated(%d), deleted(%d)",
		collID.Database, collID.Collection, docsCount, duration,
		result.UpsertedCount, result.MatchedCount, result.DeletedCount)
}

func (s *sinker) makeWriteModel(chgItem *abstract.ChangeItem, id documentID, docKey bson.M, shardKey *shardedCollectionSinkContext) (mongo.WriteModel, error) {
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
