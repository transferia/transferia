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
	item     int        // item is the index of the change item among the ones the write was prepared from.
	isolated bool       // isolated marks a model which must not share a bulk with others (see shardedCollectionSinkContext).
}

// itemsToPreparedWrites prepares the writes of the change items, in the incoming order: one per item, except for a
// document patch, which may take several writes or none (see makeUpdateModels).
// requireMetadata stays true for every portion of a collection batch containing patches.
func (s *sinker) itemsToPreparedWrites(
	ctx context.Context,
	namespace Namespace,
	items []abstract.ChangeItem,
	requireMetadata bool,
) ([]preparedWrite, error) {
	requireMetadata = requireMetadata || hasDocumentPatches(items)
	sharding, err := newShardedCollectionSinkContext(ctx, s.client, namespace, s.logger, requireMetadata)
	if err != nil {
		return nil, xerrors.Errorf("cannot prepare sharding context: %w", err)
	}
	return s.itemsToPreparedWritesWithSharding(ctx, namespace, items, sharding)
}

func (s *sinker) itemsToPreparedWritesWithSharding(
	ctx context.Context,
	namespace Namespace,
	items []abstract.ChangeItem,
	sharding *shardedCollectionSinkContext,
) ([]preparedWrite, error) {
	var origin []int // origin is the index each item had before resolvePatches; nil when nothing was dropped.
	if sharding.Enabled() && !sharding.IsTrivial() {
		// Only patches which may change the shard key need full documents for routed writes.
		var err error
		if items, origin, err = s.resolvePatches(ctx, namespace, items, sharding.KeyFields()); err != nil {
			return nil, xerrors.Errorf("cannot resolve patches for sharded %v: %w", namespace.GetFullName(), err)
		}
	}

	docIDs, err := extractDocumentIDs(items)
	if err != nil {
		return nil, xerrors.Errorf("cannot extract documents _id from change items for %v: %w", namespace.GetFullName(), err)
	}

	if sharding.Enabled() && !sharding.IsTrivial() {
		if err := sharding.Init(ctx, docIDs); err != nil {
			return nil, xerrors.Errorf("cannot init context for sharded collection %v: %w", namespace.GetFullName(), err)
		}
	}

	writes := make([]preparedWrite, 0, len(items))
	for i, item := range items {
		var docKey bson.M
		isolated := false

		if sharding.Enabled() && !sharding.IsTrivial() && item.Kind != abstract.MongoUpdateDocumentKind {
			var err error
			docKey, isolated, err = sharding.GetDocumentKey(docIDs[i], &item)
			if err != nil {
				return nil, xerrors.Errorf("cannot get document(with _id=%v) key from sharded collection %v: %w", docIDs[i].String, namespace.GetFullName(), err)
			}
		}
		models, err := s.makeWriteModels(&item, docIDs[i], docKey, sharding)
		if err != nil {
			return nil, xerrors.Errorf("cannot prepare item to write into collection %v: %w", namespace.GetFullName(), err)
		}
		// Several writes of an item share the document, so the splitter keeps their order.
		itemIndex := i
		if origin != nil {
			itemIndex = origin[i]
		}
		for _, model := range models {
			writes = append(writes, preparedWrite{model: model, docID: docIDs[i], isolated: isolated, item: itemIndex})
		}
	}
	return writes, nil
}

// splitItemsToBulkOperations distributes items into slice of bulks by hashing the _id, so every row for a given
// document will always be in the same bulk and keeps its event order; repeated writes of a document are folded
// into one where possible (see bulkSplitter). An ordered list of bulks is returned.
func (s *sinker) splitItemsToBulkOperations(
	ctx context.Context,
	namespace Namespace,
	items []abstract.ChangeItem,
	requireMetadata bool,
) ([][][]mongo_driver.WriteModel, int, error) {
	startPrepare := time.Now()

	writes, err := s.itemsToPreparedWrites(ctx, namespace, items, requireMetadata)
	if err != nil {
		return nil, 0, err
	}

	// The indexes of the collection are listed only when there is something to fold.
	fold := !s.orderedWrites() && hasRepeatedDocument(writes)
	if fold {
		unique, indexErr := hasUniqueSecondaryIndex(ctx, s.client.Database(namespace.Database).Collection(namespace.Collection))
		if indexErr != nil {
			return nil, 0, xerrors.Errorf("cannot check indexes of %v: %w", namespace.GetFullName(), indexErr)
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
		namespace.GetFullName(), len(items), len(result), totalBulks, folded, prepareDuration)
	return result, folded, nil
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
func (s *sinker) bulkWrite(ctx context.Context, namespace Namespace, bulk []mongo_driver.WriteModel) error {
	coll := s.client.Database(namespace.Database).Collection(namespace.Collection)
	opts := mongo_options.BulkWrite().SetOrdered(s.orderedWrites())

	docsNumber := len(bulk)
	startWrite := time.Now()
	result, err := s.bulkWriteWithFallbacks(ctx, namespace, coll, bulk, opts)
	if err != nil {
		if strings.Contains(err.Error(), "is too large") {
			s.logger.Warnf("BulkWrite(%v documents) to %v failed: %v. Try serial push instead.", docsNumber, namespace.GetFullName(), err)

			serialPushResult, serialPushErr := s.serialPush(ctx, namespace, bulk)
			if serialPushErr != nil {
				return xerrors.Errorf("cannot write %v documents to %v neither by BulkWrite(%v) nor by serial push: %w", docsNumber, namespace.GetFullName(), err, serialPushErr)
			}
			result = serialPushResult
		} else if strings.Contains(err.Error(), "could not extract exact shard key") {
			return xerrors.Errorf("cannot write %v documents to sharded collection %v - check if user has clusterManager or mdbShardingManager role: %w", docsNumber, namespace.GetFullName(), err)
		} else if code := codeIfIsTooLargeError(err); code != "" {
			return coded.Errorf(code, "bulk write failed for %v: %w", namespace.GetFullName(), err)
		} else {
			return xerrors.Errorf("bulk write of %v documents to %v failed: %w", docsNumber, namespace.GetFullName(), err)
		}
	}
	s.setBulkWriteMetrics(namespace, time.Since(startWrite), docsNumber, result)
	return nil
}

// bulkWriteWithFallbacks runs the bulk and re-issues the writes the target rejected for a reason a full document
// cures, as replace-upserts:
//   - an insert rejected as a duplicate (a re-delivered event after a restart, a snapshot retry) is re-issued with
//     its own document, so inserts stay idempotent;
//   - a patch the document of the target does not accept (see refetchModels) is re-issued with the document read
//     from the source.
//
// Patches which found no document are checked by healUnmatchedPatches. Any other failure is returned as is, with
// the counters of what did get written.
func (s *sinker) bulkWriteWithFallbacks(
	ctx context.Context,
	namespace Namespace,
	coll *mongo_driver.Collection,
	bulk []mongo_driver.WriteModel,
	opts *mongo_options.BulkWriteOptions,
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
			updateBulkWriteResult(&total, result)
		}
		if err == nil {
			if healErr := s.healUnmatchedPatches(ctx, namespace, coll, bulk, result, &total); healErr != nil {
				return &total, xerrors.Errorf("cannot restore documents of %s: %w", namespace.GetFullName(), healErr)
			}
			return &total, nil
		}
		var bulkErr mongo_driver.BulkWriteException
		if !xerrors.As(err, &bulkErr) || len(bulkErr.WriteErrors) == 0 || bulkErr.WriteConcernError != nil {
			return &total, xerrors.Errorf("bulk write to %s failed: %w", namespace.GetFullName(), err)
		}
		var retry []mongo_driver.WriteModel
		if s.orderedWrites() {
			// An ordered bulk stops at the first failure: the failed op and everything after it are still to be
			// written.
			failed := bulkErr.WriteErrors[0]
			done := bulk[:failed.Index]
			if healErr := s.healUnmatchedPatches(ctx, namespace, coll, done, result, &total); healErr != nil {
				return &total, xerrors.Errorf("cannot restore documents of %s: %w", namespace.GetFullName(), healErr)
			}
			switch {
			case insertToUpsertFallback(failed.Request) != nil && mongo_driver.IsDuplicateKeyError(failed):
				// Every insert of the tail is turned into an upsert at once, not only the failed one: after a
				// restart the whole tail is likely a re-delivery, and re-issuing one duplicate per round trip
				// would replay it an op at a time.
				retry = make([]mongo_driver.WriteModel, 0, len(bulk)-failed.Index)
				for _, model := range bulk[failed.Index:] {
					if fallback := insertToUpsertFallback(model); fallback != nil {
						retry = append(retry, fallback)
					} else {
						retry = append(retry, model)
					}
				}
				base += failed.Index
			case s.canRefetch(failed):
				refetchErr := s.writeRefetched(ctx, namespace, coll, []mongo_driver.WriteModel{failed.Request}, &total)
				if refetchErr != nil {
					return &total, xerrors.Errorf("ordered bulk write to %s failed at op %d: %v, so did the refetch: %w",
						namespace.GetFullName(), base+failed.Index, err, refetchErr)
				}
				retry = bulk[failed.Index+1:]
				base += failed.Index + 1
			default:
				return &total, xerrors.Errorf("ordered bulk write to %s failed at op %d: %w", namespace.GetFullName(), base+failed.Index, err)
			}
		} else {
			var rejected, applied []mongo_driver.WriteModel
			failedOps := map[int]struct{}{}
			for _, failed := range bulkErr.WriteErrors {
				failedOps[failed.Index] = struct{}{}
				fallback := insertToUpsertFallback(failed.Request)
				if fallback != nil && mongo_driver.IsDuplicateKeyError(failed) {
					retry = append(retry, fallback)
				} else if s.canRefetch(failed) {
					rejected = append(rejected, failed.Request)
				} else {
					return &total, xerrors.Errorf("bulk write to %s failed with %d write errors: %w", namespace.GetFullName(), len(bulkErr.WriteErrors), err)
				}
			}
			for i, model := range bulk {
				if _, failed := failedOps[i]; !failed {
					applied = append(applied, model)
				}
			}
			if healErr := s.healUnmatchedPatches(ctx, namespace, coll, applied, result, &total); healErr != nil {
				return &total, xerrors.Errorf("cannot restore documents of %s: %w", namespace.GetFullName(), healErr)
			}
			if refetchErr := s.writeRefetched(ctx, namespace, coll, rejected, &total); refetchErr != nil {
				return &total, xerrors.Errorf("bulk write to %s failed with %d write errors: %v, so did the refetch: %w",
					namespace.GetFullName(), len(bulkErr.WriteErrors), err, refetchErr)
			}
		}
		if len(retry) == 0 {
			return &total, nil
		}
		// The loop ends: an unordered retry holds replace-upserts only, which are not retried again; an ordered
		// one either turns its inserts into replace-upserts, once, or drops the failed patch.
		bulk = retry
	}
}

// serialPush writes models one by one.
func (s *sinker) serialPush(ctx context.Context, namespace Namespace, bulk []mongo_driver.WriteModel) (*mongo_driver.BulkWriteResult, error) {
	coll := s.client.Database(namespace.Database).Collection(namespace.Collection)
	opts := mongo_options.BulkWrite().SetOrdered(s.orderedWrites())
	totalResult := mongo_driver.BulkWriteResult{}
	for i, m := range bulk {
		iResult, iErr := s.bulkWriteWithFallbacks(ctx, namespace, coll, []mongo_driver.WriteModel{m}, opts)
		if iErr != nil {
			return nil, xerrors.Errorf("failed push %v-th document: %w", i, iErr)
		}
		updateBulkWriteResult(&totalResult, iResult)
	}
	return &totalResult, nil
}

// retryCollectionOrdered is the E11000 fallback: re-applies the items document by document in the source order,
// where a write freeing a unique index value precedes its taker; items still failing retry in rounds.
func (s *sinker) retryCollectionOrdered(
	ctx context.Context,
	namespace Namespace,
	items []abstract.ChangeItem,
	requireMetadata bool,
) error {
	windows := func(pending []abstract.ChangeItem) ([][]abstract.ChangeItem, error) {
		return s.patchPortions(ctx, namespace, pending)
	}
	return s.retryOrdered(ctx, namespace, items, requireMetadata, windows)
}

// retryOrdered prepares the writes a window of items at a time, so the source documents read in place of patches
// (see resolvePatches) are held for one window only. A document which failed holds back its later items in every
// window of the round; there is no progress only when a round over all the windows applies nothing.
func (s *sinker) retryOrdered(
	ctx context.Context,
	namespace Namespace,
	items []abstract.ChangeItem,
	requireMetadata bool,
	windows func(pending []abstract.ChangeItem) ([][]abstract.ChangeItem, error),
) error {
	coll := s.client.Database(namespace.Database).Collection(namespace.Collection)
	startWrite := time.Now()
	totalResult := mongo_driver.BulkWriteResult{}
	written := 0
	requireMetadata = requireMetadata || hasDocumentPatches(items)

	pending := items
	nextModel := make([]int, len(items))
	for len(pending) > 0 {
		writtenBefore := written
		var notPushed []abstract.ChangeItem
		var notPushedNextModel []int
		failedIDs := set.New[string]()
		windowStart := 0
		portions, err := windows(pending)
		if err != nil {
			return xerrors.Errorf("cannot split ordered retry of %v: %w", namespace.GetFullName(), err)
		}
		for _, window := range portions {
			writes, err := s.itemsToPreparedWrites(ctx, namespace, window, requireMetadata)
			if err != nil {
				return xerrors.Errorf("cannot prepare ordered retry of %v: %w", namespace.GetFullName(), err)
			}
			held := set.New[int]() // held are the items of the window with a write not applied.
			modelItem, modelIndex := -1, 0
			for _, write := range writes {
				if write.item != modelItem {
					modelItem, modelIndex = write.item, 0
				} else {
					modelIndex++
				}
				itemIndex := windowStart + write.item
				// Keep only the position of the completed prefix, without retaining prepared documents.
				if modelIndex < nextModel[itemIndex] {
					continue
				}
				if failedIDs.Contains(write.docID.String) {
					held.Add(write.item) // the items can contain many writes of a document.
					continue
				}
				pushed, err := s.retryWrite(ctx, namespace, coll, write, &totalResult)
				if err != nil {
					return xerrors.Errorf("ordered retry write to %s failed: %w", namespace.GetFullName(), err)
				}
				if pushed {
					written++
					nextModel[itemIndex] = modelIndex + 1
					continue
				}
				failedIDs.Add(write.docID.String)
				held.Add(write.item)
			}
			for i := range window {
				if held.Contains(i) {
					notPushed = append(notPushed, window[i])
					notPushedNextModel = append(notPushedNextModel, nextModel[windowStart+i])
				}
			}
			windowStart += len(window)
		}
		if len(notPushed) == len(pending) && written == writtenBefore {
			return xerrors.Errorf("ordered retry of %v cannot make progress: %d documents keep failing with duplicate key",
				namespace.GetFullName(), failedIDs.Len())
		}
		pending = notPushed
		nextModel = notPushedNextModel
	}
	s.setBulkWriteMetrics(namespace, time.Since(startWrite), written, &totalResult)
	return nil
}

// retryWrite applies one write of the ordered retry; it is not pushed when a unique index still rejects it.
func (s *sinker) retryWrite(
	ctx context.Context,
	namespace Namespace,
	coll *mongo_driver.Collection,
	write preparedWrite,
	total *mongo_driver.BulkWriteResult,
) (bool, error) {
	result, err := s.bulkWriteWithFallbacks(ctx, namespace, coll, []mongo_driver.WriteModel{write.model}, mongo_options.BulkWrite())
	if err == nil {
		updateBulkWriteResult(total, result)
		return true, nil
	}
	if !mongo_driver.IsDuplicateKeyError(err) {
		if code := codeIfIsTooLargeError(err); code != "" {
			return false, coded.Errorf(code, "write failed: %w", err)
		}
		return false, xerrors.Errorf("write failed: %w", err)
	}
	if !isPatchModel(write.model) || s.source == nil {
		return false, nil
	}
	// The patch may bring back a value which another document has taken since; the current document does not
	// hold it.
	refetchErr := s.writeRefetched(ctx, namespace, coll, []mongo_driver.WriteModel{write.model}, total)
	if refetchErr == nil {
		return true, nil
	}
	if !mongo_driver.IsDuplicateKeyError(refetchErr) {
		return false, xerrors.Errorf("refetch failed: %w", refetchErr)
	}
	return false, nil
}

func updateBulkWriteResult(totalResult *mongo_driver.BulkWriteResult, partialResult *mongo_driver.BulkWriteResult) {
	if partialResult == nil {
		return
	}
	totalResult.InsertedCount += partialResult.InsertedCount
	totalResult.MatchedCount += partialResult.MatchedCount
	totalResult.ModifiedCount += partialResult.ModifiedCount
	totalResult.DeletedCount += partialResult.DeletedCount
	totalResult.UpsertedCount += partialResult.UpsertedCount
}

func (s *sinker) setBulkWriteMetrics(namespace Namespace, duration time.Duration, docsCount int, result *mongo_driver.BulkWriteResult) {
	s.metrics.RecordDuration("bulkWrite", duration)
	s.metrics.Table(namespace.Collection, "rows", docsCount)
	s.metrics.Table(namespace.Collection, "deleted_rows", int(result.DeletedCount))
	s.metrics.Table(namespace.Collection, "upserted_rows", int(result.UpsertedCount))
	s.metrics.Table(namespace.Collection, "updated_rows", int(result.MatchedCount))
	s.logger.Debugf(
		"%v.%v: %d documents written in %v: upserted(%d), updated(%d), deleted(%d)",
		namespace.Database, namespace.Collection, docsCount, duration,
		result.UpsertedCount, result.MatchedCount, result.DeletedCount)
}

// makeWriteModels makes the writes of a change item, to be applied in the returned order.
func (s *sinker) makeWriteModels(
	chgItem *abstract.ChangeItem,
	id documentID,
	docKey bson.M,
	shardKey *shardedCollectionSinkContext,
) ([]mongo_driver.WriteModel, error) {
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
			filter := makeDocumentFilter(id.Raw, docKey, shardKey.KeyFields())
			return []mongo_driver.WriteModel{makeReplaceModel(filter, document)}, nil
		}
		if !containsInRoot(document, "_id") {
			document = append(bson.D{{Key: "_id", Value: id.Raw}}, document...)
		}
		// A plain insert costs the server less than upserting a new _id (inserts of a bulk share a storage
		// transaction, upserts get one each). A re-delivered insert falls back to the upsert in
		// bulkWriteWithFallbacks.
		return []mongo_driver.WriteModel{makeInsertModel(document)}, nil
	case abstract.UpdateKind:
		filter := makeDocumentFilter(id.Raw, docKey, shardKey.KeyFields())
		document, err := getDocument(chgItem, shardKey)
		if err != nil {
			return nil, xerrors.Errorf("cannot compose MongoDB document from ChangeItem: %w", err)
		}
		return []mongo_driver.WriteModel{makeReplaceModel(filter, document)}, nil
	case abstract.DeleteKind:
		filter := makeDocumentFilter(id.Raw, nil, nil)
		return []mongo_driver.WriteModel{makeDeleteModel(filter)}, nil
	case abstract.MongoUpdateDocumentKind:
		updateItem, err := NewUpdateDocumentChangeItem(chgItem)
		if err != nil {
			return nil, abstract.NewFatalError(xerrors.Errorf("invalid MongoUpdateDocumentKind item: %w", err))
		}
		// Retained patches do not change shard keys. Identity also finds documents relocated by a refetch.
		filter := makeDocumentFilter(id.Raw, nil, nil)
		return makeUpdateModels(
			filter,
			updateItem.UpdatedFields(),
			updateItem.RemovedFields(),
			updateItem.TruncatedArrays(),
		), nil
	default:
		return nil, makeErrOperationKindNotSupported(chgItem)
	}
}
