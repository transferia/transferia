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
	"github.com/transferia/transferia/pkg/providers/mongo/mongoerrors"
	"github.com/transferia/transferia/pkg/util/set"
	"go.mongodb.org/mongo-driver/bson"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
)

const sinkWriteConcurrency = 16 // sinkWriteConcurrency is maximum number of shards flushed concurrently per collection.

type preparedWrite struct {
	model    mongo_driver.WriteModel
	docID    documentID // docID is document _id.
	isolated bool       // isolated marks a model which must not share a bulk with others (see shardedCollectionSinkContext).
}

// itemsToPreparedWrites prepares a write per change item, in the incoming order.
func (s *sinker) itemsToPreparedWrites(
	ctx context.Context, collID Namespace, items []abstract.ChangeItem,
) ([]preparedWrite, error) {
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

	writes := make([]preparedWrite, len(items))
	for i, item := range items {
		writes[i].docID = docIDs[i]

		var docKey bson.M

		if sharding.Enabled() && !sharding.IsTrivial() {
			var err error
			docKey, writes[i].isolated, err = sharding.GetDocumentKey(docIDs[i], &item)
			if err != nil {
				return nil, xerrors.Errorf("cannot get document(with _id=%v) key from sharded collection %v: %w", docIDs[i].String, collID.GetFullName(), err)
			}
		}
		writes[i].model, err = s.makeWriteModel(&item, docIDs[i], docKey, sharding)
		if err != nil {
			return nil, xerrors.Errorf("cannot prepare item to write into collection %v: %w", collID.GetFullName(), err)
		}
	}
	return writes, nil
}

// splitItemsToBulkOperations distributes items into slice of bulks by hashing the _id, so every row for a given
// document will always be in the same bulk and keeps its event order; repeated writes of a document are folded
// into one where possible (see bulkSplitter). An ordered list of bulks is returned.
func (s *sinker) splitItemsToBulkOperations(ctx context.Context, collID Namespace, items []abstract.ChangeItem) ([][][]mongo_driver.WriteModel, error) {
	startPrepare := time.Now()

	writes, err := s.itemsToPreparedWrites(ctx, collID, items)
	if err != nil {
		return nil, err
	}

	// The indexes of the collection are listed only when there is something to fold.
	fold := !s.orderedWrites() && hasRepeatedDocument(writes)
	if fold {
		unique, indexErr := hasUniqueSecondaryIndex(ctx, s.client.Database(collID.Database).Collection(collID.Collection))
		if indexErr != nil {
			return nil, xerrors.Errorf("cannot check indexes of %v: %w", collID.GetFullName(), indexErr)
		}
		fold = !unique
	}
	splitters := make([]bulkSplitter, s.writeConcurrency())
	for i := range splitters {
		splitters[i] = newBulkSplitter(fold)
	}
	for _, write := range writes {
		splitters[shardOfDocumentID(write.docID.String, len(splitters))].Add(write.model, write.docID, write.isolated)
	}
	result := make([][][]mongo_driver.WriteModel, 0, len(splitters))
	totalBulks, folded := 0, 0
	for i := range splitters {
		folded += splitters[i].Folded()
		if bulks := splitters[i].Get(); len(bulks) > 0 {
			result = append(result, bulks)
			totalBulks += len(bulks)
		}
	}

	prepareDuration := time.Since(startPrepare)
	s.metrics.RecordDuration("bulkPrepare", prepareDuration)
	s.logger.Debugf(
		"%v: %d items prepared into %d shard(s), %d bulk(s), %d folded into an earlier write of the same document, in %v",
		collID.GetFullName(), len(items), len(result), totalBulks, folded, prepareDuration)
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

// hasRepeatedDocument reports whether some document is written more than once, i.e. whether there is anything to
// fold.
func hasRepeatedDocument(writes []preparedWrite) bool {
	seen := set.New[string]()
	for _, write := range writes {
		if seen.Contains(write.docID.String) {
			return true
		}
		seen.Add(write.docID.String)
	}
	return false
}

// indexSpec is the part of a listIndexes document the fold decision needs.
type indexSpec struct {
	Key           bson.D `bson:"key"`
	Unique        bool   `bson:"unique"`
	PrepareUnique bool   `bson:"prepareUnique"` // the index rejects new duplicates while being converted to unique
}

// hasUniqueSecondaryIndex reports whether the collection has a unique index besides the one on _id (see
// bulkSplitter on why such a collection is not folded). A collection that does not exist yet has none.
func hasUniqueSecondaryIndex(ctx context.Context, coll *mongo_driver.Collection) (bool, error) {
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return false, xerrors.Errorf("cannot list indexes: %w", err)
	}
	var specs []indexSpec
	if err := cursor.All(ctx, &specs); err != nil {
		return false, xerrors.Errorf("cannot read indexes: %w", err)
	}
	return uniqueSecondaryIndex(specs), nil
}

func uniqueSecondaryIndex(specs []indexSpec) bool {
	for _, spec := range specs {
		if (spec.Unique || spec.PrepareUnique) && !(len(spec.Key) == 1 && spec.Key[0].Key == "_id") {
			return true
		}
	}
	return false
}

func (s *sinker) orderedWrites() bool {
	return s.config.OrderedWrites
}

// writeConcurrency is the number of write shards (bulk writers) per collection.
func (s *sinker) writeConcurrency() int {
	if s.orderedWrites() {
		return 1
	}
	return sinkWriteConcurrency
}

// codeIfIsTooLargeError maps server size limit errors to user-facing error codes.
func codeIfIsTooLargeError(err error) coded.Code {
	var serverErr mongo_driver.ServerError
	if !xerrors.As(err, &serverErr) {
		return ""
	}
	if serverErr.HasErrorCode(mongoerrors.StringTooLongCode) {
		return error_codes.MongoCollectionKeyTooLarge
	}
	if serverErr.HasErrorCode(mongoerrors.BSONObjectTooLargeCode) {
		return error_codes.MongoBSONObjectTooLarge
	}
	return ""
}

// bulkWrite writes one bulk. All operations target distinct _ids (guaranteed by bulkSplitter), so Ordered(false) is safe – MongoDB can apply them in any order.
// With OrderedWrites the bulk is ordered so that a failed write does not let the following ones overtake it.
func (s *sinker) bulkWrite(ctx context.Context, collID Namespace, bulk []mongo_driver.WriteModel) error {
	coll := s.client.Database(collID.Database).Collection(collID.Collection)
	opts := mongo_options.BulkWrite().SetOrdered(s.orderedWrites())

	docsNumber := len(bulk)
	startWrite := time.Now()
	result, err := s.bulkWriteWithInsertFallback(ctx, collID, coll, bulk, opts)
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
		} else if code := codeIfIsTooLargeError(err); code != "" {
			return coded.Errorf(code, "bulk write failed for %v: %w", collID.GetFullName(), err)
		} else {
			return xerrors.Errorf("bulk write of %v documents to %v failed: %w", docsNumber, collID.GetFullName(), err)
		}
	}
	s.setBulkWriteMetrics(collID, time.Since(startWrite), docsNumber, result)
	return nil
}

// bulkWriteWithInsertFallback runs the bulk, re-issuing inserts rejected as duplicates (a re-delivered event after
// a restart, a snapshot retry) as replace-upserts of the same document, so inserts stay idempotent. Any other
// failure is returned as is, with the counters of what did get written.
func (s *sinker) bulkWriteWithInsertFallback(
	ctx context.Context, collID Namespace, coll *mongo_driver.Collection, bulk []mongo_driver.WriteModel, opts *mongo_options.BulkWriteOptions,
) (*mongo_driver.BulkWriteResult, error) {
	total := mongo_driver.BulkWriteResult{}
	base := 0 // index of bulk[0] in the bulk originally passed in, for the ordered tail retries
	for {
		result, err := coll.BulkWrite(ctx, bulk, opts)
		if result != nil {
			if err != nil && s.orderedWrites() {
				// An ordered bulk stopped by an error returns raw counters: the driver subtracts the upserts from
				// MatchedCount only for a bulk that ran to the end.
				result.MatchedCount -= result.UpsertedCount
			}
			updateBulkWriteResult(&total, *result)
		}
		if err == nil {
			return &total, nil
		}
		var bulkErr mongo_driver.BulkWriteException
		if !xerrors.As(err, &bulkErr) || len(bulkErr.WriteErrors) == 0 || bulkErr.WriteConcernError != nil {
			return &total, xerrors.Errorf("bulk write to %s failed: %w", collID.GetFullName(), err)
		}
		var retry []mongo_driver.WriteModel
		if s.orderedWrites() {
			// An ordered bulk stops at the first failure: the failed op and everything after it are still to be
			// written. Every insert of the tail is turned into an upsert at once, not only the failed one: after a
			// restart the whole tail is likely a re-delivery, and re-issuing one duplicate per round trip would
			// replay it an op at a time.
			failed := bulkErr.WriteErrors[0]
			if insertToUpsertFallback(failed.Request) == nil || !mongo_driver.IsDuplicateKeyError(failed) {
				return &total, xerrors.Errorf("ordered bulk write to %s failed at op %d: %w", collID.GetFullName(), base+failed.Index, err)
			}
			base += failed.Index
			retry = make([]mongo_driver.WriteModel, 0, len(bulk)-failed.Index)
			for _, model := range bulk[failed.Index:] {
				if fallback := insertToUpsertFallback(model); fallback != nil {
					retry = append(retry, fallback)
				} else {
					retry = append(retry, model)
				}
			}
		} else {
			for _, failed := range bulkErr.WriteErrors {
				fallback := insertToUpsertFallback(failed.Request)
				if fallback == nil || !mongo_driver.IsDuplicateKeyError(failed) {
					return &total, xerrors.Errorf("bulk write to %s failed with %d write errors: %w", collID.GetFullName(), len(bulkErr.WriteErrors), err)
				}
				retry = append(retry, fallback)
			}
		}
		bulk = retry // replace-upserts cannot fail as duplicates of _id, so the loop ends within a pass per insert
	}
}

// serialPush writes models one by one.
func (s *sinker) serialPush(ctx context.Context, collID Namespace, bulk []mongo_driver.WriteModel) (*mongo_driver.BulkWriteResult, error) {
	coll := s.client.Database(collID.Database).Collection(collID.Collection)
	opts := mongo_options.BulkWrite().SetOrdered(s.orderedWrites())
	totalResult := mongo_driver.BulkWriteResult{}
	for i, m := range bulk {
		iResult, iErr := s.bulkWriteWithInsertFallback(ctx, collID, coll, []mongo_driver.WriteModel{m}, opts)
		if iErr != nil {
			return nil, xerrors.Errorf("failed push %v-th document: %w", i, iErr)
		}
		updateBulkWriteResult(&totalResult, *iResult)
	}
	return &totalResult, nil
}

// retryCollectionOrdered is the E11000 fallback: re-applies the batch document by document in the source order,
// where a write freeing a unique index value precedes its taker; ops still failing retry in rounds.
func (s *sinker) retryCollectionOrdered(ctx context.Context, collID Namespace, items []abstract.ChangeItem) error {
	writes, err := s.itemsToPreparedWrites(ctx, collID, items)
	if err != nil {
		return xerrors.Errorf("cannot prepare ordered retry of %v: %w", collID.GetFullName(), err)
	}
	coll := s.client.Database(collID.Database).Collection(collID.Collection)
	startWrite := time.Now()
	totalResult := mongo_driver.BulkWriteResult{}

	toPush := writes
	for len(toPush) > 0 {
		var notPushed []preparedWrite
		failedIDs := set.New[string]()
		for _, write := range toPush {
			if failedIDs.Contains(write.docID.String) {
				notPushed = append(notPushed, write) // `items` can contain many items for same docID.
				continue
			}
			result, err := s.bulkWriteWithInsertFallback(ctx, collID, coll, []mongo_driver.WriteModel{write.model}, mongo_options.BulkWrite())
			if err == nil {
				updateBulkWriteResult(&totalResult, *result)
				continue
			}
			if !mongo_driver.IsDuplicateKeyError(err) {
				if code := codeIfIsTooLargeError(err); code != "" {
					return coded.Errorf(code, "ordered retry write to %s failed: %w", collID.GetFullName(), err)
				}
				return xerrors.Errorf("ordered retry write to %s failed: %w", collID.GetFullName(), err)
			}
			failedIDs.Add(write.docID.String)
			notPushed = append(notPushed, write)
		}
		if len(notPushed) == len(toPush) {
			return xerrors.Errorf("ordered retry of %v cannot make progress: %d documents keep failing with duplicate key",
				collID.GetFullName(), len(notPushed))
		}
		toPush = notPushed
	}
	s.setBulkWriteMetrics(collID, time.Since(startWrite), len(writes), &totalResult)
	return nil
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
	case abstract.InsertKind:
		document, err := getDocument(chgItem, shardKey)
		if err != nil {
			return nil, xerrors.Errorf("cannot compose MongoDB document from ChangeItem: %w", err)
		}
		if shardKey.Enabled() && !shardKey.IsTrivial() {
			// Sharded by more than _id: the document may have moved to another shard since the event, and only the
			// upsert by its stored key finds it there; a plain insert would land a second copy on the shard of the
			// event's key.
			return makeReplaceModel(makeDocumentFilter(id.Raw, docKey, shardKey.KeyFields()), document), nil
		}
		if !containsInRoot(document, "_id") {
			document = append(bson.D{{Key: "_id", Value: id.Raw}}, document...)
		}
		// A plain insert costs the server less than upserting a new _id (inserts of a bulk share a storage
		// transaction, upserts get one each). A re-delivered insert falls back to the upsert in
		// bulkWriteWithInsertFallback.
		return makeInsertModel(document), nil
	case abstract.UpdateKind:
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
