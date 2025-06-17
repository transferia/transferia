//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.ytsaurus.tech/library/go/core/log"
)

type keyDescriptor struct {
	namespace   Namespace
	keyAsString string // this is mongocommon.DocumentKey but in string representation
}

func makeKeyDescriptor(namespace Namespace, key interface{}) keyDescriptor {
	idStr := fmt.Sprintf("%v", key)
	return keyDescriptor{
		namespace:   namespace,
		keyAsString: idStr,
	}
}

type keyBatch struct {
	batch     map[keyDescriptor]keyChangeEvent
	batchSize uint
}

type sizedFullDocument struct {
	document   bson.D
	decodeTime time.Duration
	size       int
}

// FullDocumentExtractor to get default instance of this type use MakeDefaultFullDocumentExtractor
type FullDocumentExtractor func(ctx context.Context, ns Namespace, keyList bson.A) ([]sizedFullDocument, error)

// MakeDefaultFullDocumentExtractor Constructs default document extractor for batcher that uses Mongo connection
func MakeDefaultFullDocumentExtractor(client *MongoClientWrapper) FullDocumentExtractor {
	return func(ctx context.Context, ns Namespace, keyList bson.A) ([]sizedFullDocument, error) {
		filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: keyList}}}}

		// get cursor
		var collectionOptions options.CollectionOptions
		collectionOptions.SetReadConcern(readconcern.Local())
		cs, err := client.Database(ns.Database).Collection(ns.Collection, &collectionOptions).Find(ctx, filter)
		if err != nil {
			return nil, xerrors.Errorf("Cannot open pure cursor for namespace %s: %w", ns, err)
		}

		resultingSlice := []sizedFullDocument{}
		for cs.Next(ctx) {
			var fullDocument bson.D
			st := time.Now()
			if err := cs.Decode(&fullDocument); err != nil {
				return nil, xerrors.Errorf("Cannot decode full document: %w", err)
			}
			decodeTime := time.Since(st)

			resultingSlice = append(resultingSlice, sizedFullDocument{
				document:   fullDocument,
				decodeTime: decodeTime,
				size:       len(cs.Current),
			})
		}
		if cs.Err() != nil {
			return nil, xerrors.Errorf("An error occured when retrieving documents from pure cursor: %w", cs.Err())
		}
		return resultingSlice, nil
	}
}

// keyBatcher accepts change events via method PushKeyChangeEvent
// it uses namespace, key, size of key and timestamp in order to form
// batch of keys, then it uploads documents directly from source
// database and produce new change events that dispatch into specified
// in NewKeyBatcher's fullDocumentPusher argument
type keyBatcher struct {
	logger                log.Logger
	fullDocumentExtractor FullDocumentExtractor
	pusher                changeEventPusher // receiver of full document events

	batchMutex           sync.Mutex // protects: batch, batchSize, batchByteSize, maxKeySize, batchCollapseCounter
	batch                map[keyDescriptor]keyChangeEvent
	batchSize            uint
	batchByteSize        uint64
	maxKeySize           uint64 // for statistics
	batchCollapseCounter uint64 // for staticsics

	parameters BatcherParameters

	lastFlush    time.Time
	flusherEnded chan struct{}
	flusherError error
	closeOnce    sync.Once

	ctx    context.Context
	cancel func()
}

// Close do not forget to defer Close() this resource
func (f *keyBatcher) Close() error {
	f.closeOnce.Do(func() {
		f.cancel()
		// join periodicFlusher
		<-f.flusherEnded
	})
	if f.flusherError != nil {
		return xerrors.Errorf("flusher error occured: %w", f.flusherError)
	}
	return nil
}

func (f *keyBatcher) PushKeyChangeEvent(keyEvent *keyChangeEvent) error {
	if f.flusherError != nil {
		return xerrors.Errorf("flusher error occurred: %w", f.flusherError)
	}

	docIDEvent := keyEvent.keyEvent

	switch docIDEvent.OperationType {
	case "insert", "update", "replace", "delete", "noop":
		// postpone insert and update change events for upload from pure cursor
		err := f.putInBatch(keyEvent)
		if err != nil {
			return xerrors.Errorf("cannot put keyEvent in batch: %w", err)
		}
	default:
		// immediately flush batch on other events
		err := func() error {
			f.batchMutex.Lock()
			defer f.batchMutex.Unlock()
			return f.flush() // f.batchMutex taken
		}()
		if err != nil {
			return xerrors.Errorf("Couldn't flush batch: %w", err)
		}
		// other events are transient, but serialized in time within batches
		f.logger.Warnf("Default case branch keyEvent: %s", docIDEvent.OperationType)
		if err := f.pusher(f.ctx, keyEvent.toChangeEvent()); err != nil {
			return xerrors.Errorf("change keyEvent pusher error: %w", err)
		}
	}

	return nil
}

func (f *keyBatcher) periodicFlusher() {
	defer func() {
		f.flusherEnded <- struct{}{}
	}()

	ticker := time.NewTicker(f.parameters.BatchFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			f.logger.Info("Watcher context done in periodic flusher", log.Error(f.ctx.Err()))
			return
		case tickTime := <-ticker.C:
			if tickTime.Sub(f.lastFlush) < f.parameters.BatchFlushInterval {
				break
			}
			f.logger.Info("Too long no batch flushes, initiate batch flushing", log.Duration("duration", f.parameters.BatchFlushInterval))
			err := func() error {
				f.batchMutex.Lock()
				defer f.batchMutex.Unlock()
				// double check is flushing needed
				if time.Since(f.lastFlush) < f.parameters.BatchFlushInterval {
					return nil
				}
				return f.flush() // f.batchMutex taken
			}()
			if err != nil {
				f.flusherError = err
				return
			}
		}
	}
}

// putInBatch responsible for adding key in namespace for update.
// do not use it directly as client
// byteSize needed to track request BSON size
func (f *keyBatcher) putInBatch(chEvent *keyChangeEvent) error {
	event := chEvent.keyEvent
	byteSize := uint64(chEvent.size)
	f.batchMutex.Lock()
	defer f.batchMutex.Unlock()

	// get key descriptor
	kd := makeKeyDescriptor(event.Namespace, event.DocumentKey.ID)

	// refresh max key size
	if f.maxKeySize < byteSize {
		f.maxKeySize = byteSize
		f.logger.Info("New record of key size for collection",
			log.String("collection", event.Namespace.GetFullName()),
			log.UInt64("max_key_size", f.maxKeySize),
		)
	}

	if f.batchByteSize+byteSize >= f.parameters.KeySizeThreshold {
		// immediately send batch to processor
		err := f.flush() // f.batchMutex taken
		if err != nil {
			return xerrors.Errorf("cannot send batch on accumulated key size heuristic: %w", err)
		}
	}

	if oplogEvent, exists := f.batch[kd]; exists {
		if oplogEvent.keyEvent.ClusterTime.Compare(event.ClusterTime) <= 0 {
			// refresh key time to newer time and register collapse
			oplogEvent.keyEvent.OperationType = event.OperationType
			oplogEvent.keyEvent.ClusterTime = event.ClusterTime
			f.batch[kd] = oplogEvent
			f.batchCollapseCounter++
			if f.batchCollapseCounter%1000 == 0 {
				f.logger.Info("Document key -- batch collapse counter", log.UInt64("batch_collapses", f.batchCollapseCounter))
			}
		} else {
			// This should not happen as long as mongo oplog is serialized through time and sinker code is correct
			f.logger.Errorf("Put in batch (too old, didn't put) [%v | %s] %v", event.ClusterTime, event.OperationType, event.DocumentKey.ID)
		}
	}
	f.batch[kd] = *chEvent
	f.batchSize++
	f.batchByteSize += byteSize

	if f.batchSize >= f.parameters.BatchSizeLimit || f.batchByteSize >= f.parameters.KeySizeThreshold {
		err := f.flush() // f.batchMutex taken
		if err != nil {
			return xerrors.Errorf("cannot send batch on exceeded amount of documents heuristic: %w", err)
		}
	}
	return nil
}

// flush defers periodic flusher
// and prepares batch for uploading from source database
// WARNING!!! TAKE f.batchMutex before calling this method!!!
func (f *keyBatcher) flush() error {
	f.lastFlush = time.Now()
	batch := f.batch
	batchSize := f.batchSize
	f.batch = map[keyDescriptor]keyChangeEvent{}
	f.batchSize = 0
	f.batchByteSize = 0
	if len(batch) == 0 {
		return nil
	}
	return f.processBatch(keyBatch{
		batch:     batch,
		batchSize: batchSize,
	})
}

func groupByNamespace(batchAndMetainfo keyBatch) map[Namespace][]keyChangeEvent {
	groupBatchByNamespace := map[Namespace][]keyChangeEvent{}
	for kd, keyLastOplog := range batchAndMetainfo.batch {
		groupBatchByNamespace[kd.namespace] = append(groupBatchByNamespace[kd.namespace], keyLastOplog)
	}
	return groupBatchByNamespace
}

func newDeleteEvent(key *keyChangeEvent) *changeEvent {
	event := ChangeEvent{
		KeyChangeEvent:    *key.keyEvent,
		FullDocument:      nil,
		UpdateDescription: nil,
	}
	return &changeEvent{
		event:      &event,
		size:       key.size,
		decodeTime: key.decodeTime,
	}
}

func newUpsertEvent(key *KeyChangeEvent, sizedDoc sizedFullDocument) *changeEvent {
	event := ChangeEvent{
		KeyChangeEvent:    *key,
		FullDocument:      sizedDoc.document,
		UpdateDescription: nil,
	}
	return &changeEvent{
		event:      &event,
		size:       sizedDoc.size,
		decodeTime: sizedDoc.decodeTime,
	}
}

func (f *keyBatcher) notifyProcessedBatchStatistics(batchAndMetainfo keyBatch, changeEventSlice []*changeEvent, keysToFetch, keysFetched, deletedKeys int) {
	if keysToFetch != keysFetched {
		f.logger.Info("Cursor retrieved different amount of documents than keys in batch. This means that some documents are not existing anymore on polling by key.",
			log.Int("expected", keysToFetch),
			log.Int("actual", keysFetched),
			log.Int("deleted", deletedKeys),
			log.UInt("batchSize", batchAndMetainfo.batchSize),
		)
	} else {
		f.logger.Debug("Received batch", log.Int("expected", keysToFetch), log.Int("actual", len(changeEventSlice)))
	}
}

func (f *keyBatcher) pushUnorderedChangeEvents(changeEventSlice []*changeEvent) error {
	// sort changeEvents by timestamp time to preserve source contract
	sort.Slice(changeEventSlice, func(i, j int) bool {
		tsi := changeEventSlice[i].event.ClusterTime
		tsj := changeEventSlice[j].event.ClusterTime
		return tsi.Compare(tsj) > 0
	})
	// serially provide events to pusher
	for _, ce := range changeEventSlice {
		if err := f.pusher(f.ctx, ce); err != nil {
			return xerrors.Errorf("change keyEvent pusher error: %w", err)
		}
	}
	return nil
}

func (f *keyBatcher) processBatch(batchAndMetainfo keyBatch) error {
	// compatible with mongo 4.0 and higher
	// see: https://docs.mongodb.com/manual/reference/operator/aggregation/unionWith/#mongodb-pipeline-pipe.-unionWith
	var keysToFetch, keysFetched, deletedKeys int
	// TODO(@kry127) preallocation can be optimized: in case of single-threaded processBatch it can be allocated only once in 'f'
	changeEventSlice := make([]*changeEvent, batchAndMetainfo.batchSize)[:0]
	// group batch by namespace
	for ns, keys := range groupByNamespace(batchAndMetainfo) {
		inList := bson.A{}
		for _, key := range keys {
			switch key.keyEvent.OperationType {
			case "insert", "update", "replace":
				inList = append(inList, key.keyEvent.DocumentKey.ID)
				keysToFetch++
			default:
				// do not upload full document for events "delete", "drop", ...
				changeEventSlice = append(changeEventSlice, newDeleteEvent(&key))
				deletedKeys++
			}
		}
		if len(inList) == 0 {
			continue
		}

		sizedFullDocuments, err := f.fullDocumentExtractor(f.ctx, ns, inList)
		if err != nil {
			return xerrors.Errorf("Error with full documents extraction: %w", err)
		}
		for _, sizedFullDoc := range sizedFullDocuments {
			fullDocument := sizedFullDoc.document
			// build extracted fullDocument descriptor
			fullDocumentKD := makeKeyDescriptor(ns, fullDocument.Map()["_id"])
			// extract timestamp
			lastOplogEvent, ok := batchAndMetainfo.batch[fullDocumentKD]
			if !ok {
				return xerrors.Errorf("FullDocument from pure cursor missing anchor change keyEvent: %v", fullDocument)
			}

			changeEventSlice = append(changeEventSlice, newUpsertEvent(lastOplogEvent.keyEvent, sizedFullDoc))
			keysFetched++
		}
	}
	f.notifyProcessedBatchStatistics(batchAndMetainfo, changeEventSlice, keysToFetch, keysFetched, deletedKeys)
	return f.pushUnorderedChangeEvents(changeEventSlice)
}

// NewKeyBatcher
// nil parameters are default parameters
func NewKeyBatcher(
	ctx context.Context,
	logger log.Logger,
	fullDocumentExtractor FullDocumentExtractor,
	fullDocumentPusher changeEventPusher,
	parameters *BatcherParameters,
) (*keyBatcher, error) {
	if parameters == nil {
		return nil, abstract.NewFatalError(xerrors.New("batcher parameters required"))
	}
	ctxNew, cancelNew := context.WithCancel(ctx)
	ret := keyBatcher{
		logger:                logger,
		fullDocumentExtractor: fullDocumentExtractor,
		pusher:                fullDocumentPusher,

		batchMutex:           sync.Mutex{},
		batch:                map[keyDescriptor]keyChangeEvent{},
		batchSize:            0,
		batchByteSize:        0,
		maxKeySize:           0,
		batchCollapseCounter: 0,

		parameters: *parameters,

		lastFlush:    time.Now(),
		flusherEnded: make(chan struct{}, 1),
		flusherError: nil,
		closeOnce:    sync.Once{},

		ctx:    ctxNew,
		cancel: cancelNew,
	}
	go ret.periodicFlusher()
	return &ret, nil
}
