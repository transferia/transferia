package mongo

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mongo/mongoerrors"
	"github.com/transferia/transferia/pkg/util/set"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.ytsaurus.tech/library/go/core/log"
)

// A document patch (MongoUpdateDocumentKind) carries the changed fields only, so it relies on the document of the
// target being the one the source had before the update. That does not hold after a restart, when events are
// delivered again over a newer state, nor when the target lost the document. Both cases end the same way: the
// current document is read from the source and written whole. The events between the patch and the read are
// applied over it again, which is harmless as long as every event up to the read is applied in order: the result
// is the document as of the last event.

// sourceDocuments reads the current documents of the source of the transfer.
type sourceDocuments interface {
	// Find returns the documents of the source collection matching the target one, by the string form of their
	// _id (see documentID), in a state not older than the cluster time given.
	Find(
		ctx context.Context,
		target Namespace,
		ids []interface{},
		notBefore primitive.Timestamp,
	) (map[string]bson.D, error)
	Close(ctx context.Context) error
}

// patchMismatchCodes are the server errors telling that the document of the target is not the one the patch was
// made for.
var patchMismatchCodes = set.New(
	mongoerrors.BadValueCode,      // an array is truncated, the field holds something else
	mongoerrors.PathNotViableCode, // a field is set below a scalar, a null or an array
	mongoerrors.NonExistentPathCode,
	mongoerrors.BSONObjectTooLargeCode, // fields of different moments do not fit into a document together
)

// refetchChunk bounds the documents read from the source and held in memory at once.
const refetchChunk = 100

// canRefetch reports whether the failed write is a patch which the document of the target did not accept.
func (s *sinker) canRefetch(failed mongo_driver.BulkWriteError) bool {
	return s.source != nil && isPatchModel(failed.Request) && patchMismatchCodes.Contains(failed.Code)
}

// refetchModels returns the writes bringing the documents of the patches to their current state in the source:
// a replace-upsert per document. A document missing in the source gets none, its delete is ahead in the stream.
func (s *sinker) refetchModels(
	ctx context.Context,
	namespace Namespace,
	patches []mongo_driver.WriteModel,
) ([]mongo_driver.WriteModel, error) {
	if len(patches) == 0 {
		return nil, nil
	}
	filters := make([]bson.D, 0, len(patches))
	ids := make([]interface{}, 0, len(patches))
	docIDs := make([]documentID, 0, len(patches))
	for _, patch := range patches {
		filter, id, err := patchFilter(patch)
		if err != nil {
			return nil, xerrors.Errorf("cannot refetch a document of %v: %w", namespace.GetFullName(), err)
		}
		filters = append(filters, filter)
		ids = append(ids, id)
		docIDs = append(docIDs, documentID{Raw: id, String: documentIDString(id)})
	}
	documents, err := s.findInSource(ctx, namespace, ids)
	if err != nil {
		return nil, xerrors.Errorf("cannot find source documents for refetch: %w", err)
	}
	if len(documents) == 0 {
		return nil, nil
	}
	sharding, err := newShardedCollectionSinkContext(ctx, s.client, namespace, s.logger, true)
	if err != nil {
		return nil, xerrors.Errorf("cannot prepare sharding context for refetch: %w", err)
	}
	if sharding.Enabled() && !sharding.IsTrivial() {
		if err := sharding.Init(ctx, docIDs); err != nil {
			return nil, xerrors.Errorf("cannot read target keys for refetch of %v: %w", namespace.GetFullName(), err)
		}
	}
	models := make([]mongo_driver.WriteModel, 0, len(documents))
	for i, filter := range filters {
		if document, ok := documents[documentIDString(ids[i])]; ok {
			if sharding.Enabled() && !sharding.IsTrivial() {
				key := sharding.docKeysCache[docIDs[i].String]
				if key == nil {
					key = document.Map()
				}
				filter = makeDocumentFilter(ids[i], key, sharding.KeyFields())
				for _, field := range sharding.KeyFields() {
					if _, present := GetValueByPath(key, field); !present && !useID(field) {
						filter = append(filter, bson.E{Key: field, Value: nil})
					}
				}
			}
			models = append(models, makeReplaceModel(filter, document))
		}
	}
	s.logger.Infof("%v: %d documents refetched from the source in place of %d patches",
		namespace.GetFullName(), len(models), len(patches))
	return models, nil
}

// isRoutingSafePatch reports whether a valid patch leaves every shard-key path untouched.
func isRoutingSafePatch(item *abstract.ChangeItem, keyFields []string) bool {
	if item.Kind != abstract.MongoUpdateDocumentKind {
		return false
	}
	patch, err := NewUpdateDocumentChangeItem(item)
	if err != nil {
		return false // Preparation reports invalid patches; grouping must not retain them.
	}
	overlaps := func(path string) bool {
		for _, key := range keyFields {
			if path == key || strings.HasPrefix(path, key+".") || strings.HasPrefix(key, path+".") {
				return true
			}
		}
		return false
	}
	for _, field := range patch.UpdatedFields() {
		if overlaps(field.Key) {
			return false
		}
	}
	for _, field := range patch.RemovedFields() {
		if overlaps(field) {
			return false
		}
	}
	for _, array := range patch.TruncatedArrays() {
		if overlaps(array.Field) {
			return false
		}
	}
	return true
}

// resolvePatches resolves patches touching shard-key paths into current source documents, preserving other patches
// and the original indexes. A resolved patch missing in the source is dropped; its delete is ahead in the stream.
func (s *sinker) resolvePatches(
	ctx context.Context,
	namespace Namespace,
	items []abstract.ChangeItem,
	keyFields []string,
) ([]abstract.ChangeItem, []int, error) {
	var ids []interface{}
	seen := set.New[string]()
	for i := range items {
		if items[i].Kind != abstract.MongoUpdateDocumentKind || isRoutingSafePatch(&items[i], keyFields) {
			continue
		}
		if _, err := NewUpdateDocumentChangeItem(&items[i]); err != nil {
			return nil, nil, abstract.NewFatalError(xerrors.Errorf("invalid MongoUpdateDocumentKind item: %w", err))
		}
		id := getNativeDocumentID(&items[i])
		key := documentIDString(id)
		if seen.Contains(key) {
			continue
		}
		seen.Add(key)
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return items, nil, nil
	}
	documents, err := s.findInSource(ctx, namespace, ids)
	if err != nil {
		return nil, nil, xerrors.Errorf("cannot resolve source documents: %w", err)
	}
	resolved := make([]abstract.ChangeItem, 0, len(items))
	origin := make([]int, 0, len(items))
	for i, item := range items {
		if item.Kind == abstract.MongoUpdateDocumentKind && !isRoutingSafePatch(&item, keyFields) {
			id := getNativeDocumentID(&item)
			document, ok := documents[documentIDString(id)]
			if !ok {
				continue
			}
			item.Kind = abstract.UpdateKind
			item.ColumnNames = DocumentSchema.ColumnsNames
			item.ColumnValues = []interface{}{id, document}
			item.TableSchema = DocumentSchema.Columns
		}
		resolved = append(resolved, item)
		origin = append(origin, i)
	}
	return resolved, origin, nil
}

func hasDocumentPatches(items []abstract.ChangeItem) bool {
	for i := range items {
		if items[i].Kind == abstract.MongoUpdateDocumentKind {
			return true
		}
	}
	return false
}

// patchPortions separates retained patches from routed writes, so a refetch relocation precedes the next target-key
// lookup. Each run also bounds the documents held by resolvePatches. Only nontrivially sharded targets are split.
func (s *sinker) patchPortions(
	ctx context.Context,
	namespace Namespace,
	items []abstract.ChangeItem,
) ([][]abstract.ChangeItem, error) {
	if !hasDocumentPatches(items) {
		return [][]abstract.ChangeItem{items}, nil
	}
	sharding, err := newShardedCollectionSinkContext(ctx, s.client, namespace, s.logger, true)
	if err != nil {
		return nil, xerrors.Errorf("cannot split writes of %v: %w", namespace.GetFullName(), err)
	}
	if !sharding.Enabled() || sharding.IsTrivial() {
		return [][]abstract.ChangeItem{items}, nil
	}
	var portions [][]abstract.ChangeItem
	start, safe := 0, false
	for i := range items {
		current := isRoutingSafePatch(&items[i], sharding.KeyFields())
		if i > 0 && current != safe {
			portions = append(portions, splitByPatchedDocuments(items[start:i], refetchChunk)...)
			start = i
		}
		safe = current
	}
	return append(portions, splitByPatchedDocuments(items[start:], refetchChunk)...), nil
}

// splitByPatchedDocuments cuts the items into consecutive portions patching at most limit documents each.
func splitByPatchedDocuments(items []abstract.ChangeItem, limit int) [][]abstract.ChangeItem {
	var portions [][]abstract.ChangeItem
	start := 0
	documents := set.New[string]()
	for i := range items {
		if items[i].Kind != abstract.MongoUpdateDocumentKind {
			continue
		}
		id := documentIDString(getNativeDocumentID(&items[i]))
		if documents.Contains(id) {
			continue
		}
		if documents.Len() == limit {
			portions = append(portions, items[start:i])
			start, documents = i, set.New[string]()
		}
		documents.Add(id)
	}
	return append(portions, items[start:])
}

// findInSource returns the current documents of the source by the string form of their _id.
func (s *sinker) findInSource(
	ctx context.Context,
	namespace Namespace,
	ids []interface{},
) (map[string]bson.D, error) {
	if s.source == nil {
		return nil, xerrors.Errorf("no access to the source documents of %v", namespace.GetFullName())
	}
	documents, err := s.source.Find(ctx, namespace, ids, s.patchTime())
	if err != nil {
		return nil, xerrors.Errorf("cannot read %d source documents of %v: %w", len(ids), namespace.GetFullName(), err)
	}
	s.metrics.Table(namespace.Collection, "refetched_rows", len(documents))
	return documents, nil
}

// healUnmatchedPatches restores the documents which the patches among the written models did not find in the
// target, though the source has them. The written counters tell whether there are such documents at all; only
// then the target is asked which ones.
func (s *sinker) healUnmatchedPatches(
	ctx context.Context,
	namespace Namespace,
	coll *mongo_driver.Collection,
	written []mongo_driver.WriteModel,
	result *mongo_driver.BulkWriteResult,
	total *mongo_driver.BulkWriteResult,
) error {
	if result == nil {
		return nil
	}
	var patches []mongo_driver.WriteModel
	matching := 0 // matching is the number of models which either match a document or upsert one.
	for _, model := range written {
		switch model.(type) {
		case *mongo_driver.UpdateOneModel:
			patches = append(patches, model)
			matching++
		case *mongo_driver.ReplaceOneModel:
			matching++
		}
	}
	if len(patches) == 0 || result.MatchedCount+result.UpsertedCount >= int64(matching) {
		return nil
	}
	if s.source == nil {
		return xerrors.Errorf("%d patches found no document, and there is no access to the source",
			int64(matching)-result.MatchedCount-result.UpsertedCount)
	}

	ids := make([]interface{}, 0, len(patches))
	for _, patch := range patches {
		_, id, err := patchFilter(patch)
		if err != nil {
			return xerrors.Errorf("cannot check a patch: %w", err)
		}
		ids = append(ids, id)
	}
	cursor, err := coll.Find(
		ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		mongo_options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return xerrors.Errorf("cannot look for the patched documents: %w", err)
	}
	var found []bson.D
	if err := cursor.All(ctx, &found); err != nil {
		return xerrors.Errorf("cannot read the patched documents: %w", err)
	}
	present := set.New[string]()
	for _, document := range found {
		if id, ok := GetValueByPath(document, "_id"); ok {
			present.Add(documentIDString(id))
		}
	}
	var missing []mongo_driver.WriteModel
	for i, patch := range patches {
		if !present.Contains(documentIDString(ids[i])) {
			missing = append(missing, patch)
		}
	}
	return s.writeRefetched(ctx, namespace, coll, missing, total)
}

// writeRefetched writes the documents of the patches as they are in the source now (see refetchModels), a chunk
// at a time.
func (s *sinker) writeRefetched(
	ctx context.Context,
	namespace Namespace,
	coll *mongo_driver.Collection,
	patches []mongo_driver.WriteModel,
	total *mongo_driver.BulkWriteResult,
) error {
	for len(patches) > 0 {
		chunk := patches[:min(refetchChunk, len(patches))]
		patches = patches[len(chunk):]
		models, err := s.refetchModels(ctx, namespace, chunk)
		if err != nil {
			return xerrors.Errorf("cannot refetch documents: %w", err)
		}
		// A refetched document may change shards, which MongoDB only permits in a singleton write command.
		for _, model := range models {
			result, err := coll.BulkWrite(ctx, []mongo_driver.WriteModel{model}, mongo_options.BulkWrite())
			if err != nil {
				return xerrors.Errorf("cannot write refetched document: %w", err)
			}
			updateBulkWriteResult(total, result)
		}
	}
	return nil
}

// patchFilter returns the filter of a patch and the _id it starts with (see makeDocumentFilter).
func patchFilter(model mongo_driver.WriteModel) (bson.D, interface{}, error) {
	update, ok := model.(*mongo_driver.UpdateOneModel)
	if !ok {
		return nil, nil, xerrors.Errorf("unexpected model %T", model)
	}
	filter, ok := update.Filter.(bson.D)
	if !ok || len(filter) == 0 || filter[0].Key != "_id" {
		return nil, nil, xerrors.Errorf("unexpected filter %v", update.Filter)
	}
	return filter, filter[0].Value, nil
}

// documentIDString is the string form of an _id, the one of documentID.String.
func documentIDString(id interface{}) string {
	return fmt.Sprintf("%#v", id)
}

// lazySourceDocuments connects to the source on the first read: most transfers never need it.
type lazySourceDocuments struct {
	source *MongoSource
	logger log.Logger
	// database is the source database of every target collection when the target puts them all into one.
	database       string
	targetDatabase string

	mutex  sync.Mutex
	client *MongoClientWrapper
}

var _ sourceDocuments = (*lazySourceDocuments)(nil)

// newSourceDocuments returns nil when the source collection of a target one cannot be told: the target puts
// collections of several source databases into one. The collections are the ones the source replicates.
func newSourceDocuments(
	source *MongoSource,
	collections []MongoCollection,
	target *MongoDestination,
	logger log.Logger,
) sourceDocuments {
	database := ""
	if target.Database != "" {
		databases := set.New[string]()
		for _, collection := range collections {
			databases.Add(collection.DatabaseName)
		}
		if databases.Len() != 1 {
			return nil
		}
		database = databases.Slice()[0]
		if database == "" || database == "*" {
			return nil
		}
	}
	return &lazySourceDocuments{
		source:         source,
		logger:         logger,
		database:       database,
		targetDatabase: target.Database,
		mutex:          sync.Mutex{},
		client:         nil,
	}
}

func (d *lazySourceDocuments) connect(ctx context.Context) (*MongoClientWrapper, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.client == nil {
		client, err := Connect(ctx, d.source.ConnectionOptions(d.source.RootCAFiles), d.logger)
		if err != nil {
			return nil, xerrors.Errorf("cannot connect to the source: %w", err)
		}
		d.client = client
	}
	return d.client, nil
}

func (d *lazySourceDocuments) Find(
	ctx context.Context,
	target Namespace,
	ids []interface{},
	notBefore primitive.Timestamp,
) (map[string]bson.D, error) {
	client, err := d.connect(ctx)
	if err != nil {
		return nil, err
	}
	database := target.Database
	if d.targetDatabase != "" {
		database = d.database
	}
	collection := client.Database(database).Collection(target.Collection)
	documents, err := findCurrentDocuments(ctx, collection, ids, notBefore)
	if err != nil {
		return nil, xerrors.Errorf("cannot find documents of %v.%v: %w", database, target.Collection, err)
	}
	result := make(map[string]bson.D, len(documents))
	for _, document := range documents {
		if id, ok := GetValueByPath(document, "_id"); ok {
			result[documentIDString(id)] = document
		}
	}
	return result, nil
}

// findCurrentDocuments reads documents by _id in place of events of the change stream. The read is causally
// consistent with the cluster time of the events, so it never returns a state older than an event delivered: a
// member lagging behind waits or fails instead.
func findCurrentDocuments(
	ctx context.Context,
	collection *mongo_driver.Collection,
	ids []interface{},
	notBefore primitive.Timestamp,
) ([]bson.D, error) {
	collection, err := collection.Clone(mongo_options.Collection().SetReadConcern(readconcern.Majority()))
	if err != nil {
		return nil, xerrors.Errorf("cannot set the read concern: %w", err)
	}
	session, err := collection.Database().Client().StartSession(mongo_options.Session().SetCausalConsistency(true))
	if err != nil {
		return nil, xerrors.Errorf("cannot start a session: %w", err)
	}
	defer session.EndSession(ctx)
	if !notBefore.IsZero() {
		if err := session.AdvanceOperationTime(&notBefore); err != nil {
			return nil, xerrors.Errorf("cannot set the operation time: %w", err)
		}
	}
	var documents []bson.D
	err = mongo_driver.WithSession(ctx, session, func(sessionCtx mongo_driver.SessionContext) error {
		cursor, err := collection.Find(sessionCtx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
		if err != nil {
			return xerrors.Errorf("cannot open a cursor: %w", err)
		}
		if err := cursor.All(sessionCtx, &documents); err != nil {
			return xerrors.Errorf("cannot read the cursor: %w", err)
		}
		return nil
	})
	return documents, err
}

func (d *lazySourceDocuments) Close(ctx context.Context) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.client == nil {
		return nil
	}
	err := d.client.Close(ctx)
	d.client = nil
	return err
}

// notePatchTime remembers the latest cluster time among the patches pushed: the documents read from the source in
// their place must not be older.
func (s *sinker) notePatchTime(items []abstract.ChangeItem) {
	latest := primitive.Timestamp{T: 0, I: 0}
	for i := range items {
		if items[i].Kind != abstract.MongoUpdateDocumentKind {
			continue
		}
		at := primitive.Timestamp{T: uint32(items[i].LSN), I: uint32(items[i].Counter)}
		if at.After(latest) {
			latest = at
		}
	}
	s.patchTimeMutex.Lock()
	defer s.patchTimeMutex.Unlock()
	if latest.After(s.latestPatchTime) {
		s.latestPatchTime = latest
	}
}

func (s *sinker) patchTime() primitive.Timestamp {
	s.patchTimeMutex.Lock()
	defer s.patchTimeMutex.Unlock()
	return s.latestPatchTime
}
