//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

type localOplogRsWatcher struct {
	logger               log.Logger
	oplog                *mongo.Collection // local.oplog.rs collection
	client               *MongoClientWrapper
	filterOplogWithRegex bool

	initialTime     primitive.Timestamp
	config          *MongoSource
	shouldReplicate func(optype string, ns Namespace) bool

	fullWhereStatementCache *bson.D
}

func (w *localOplogRsWatcher) GetResumeToken() bson.Raw {
	w.logger.Errorf("collection local.oplog.rs cannot provide resume token")
	return nil
}

func (w *localOplogRsWatcher) Close(ctx context.Context) {
	// no resources are occupied (should not close mongoClient)
}

func (w *localOplogRsWatcher) Watch(ctx context.Context, pusher changeEventPusher) error {
	batcher, err := NewKeyBatcher(ctx, w.logger, MakeDefaultFullDocumentExtractor(w.client), pusher, w.config.BatchingParams)
	if err != nil {
		return xerrors.Errorf("cannot initialize batcher: %w", err)
	}
	defer func(batcher *keyBatcher) {
		if err := batcher.Close(); err != nil {
			w.logger.Error("Batcher close error", log.Error(err))
		}
	}(batcher)

	oplogTime := w.initialTime
	for {
		newTime, eventsRead, err := w.watchBatchFrom(ctx, oplogTime, batcher)
		if err != nil {
			return xerrors.Errorf("batch watching error: %w", err)
		}
		if eventsRead == 0 {
			w.logger.Info("No new events came. Wait for 5s")
			select {
			case <-ctx.Done():
				return xerrors.Errorf("Context ended: %w", ctx.Err())
			case <-time.After(5 * time.Second):
			}
		} else {
			oplogTime = newTime
		}
	}
}

func buildRegex(mongoNsSlice []string) string {
	if len(mongoNsSlice) > 1 {
		return fmt.Sprintf("^(%s)$", strings.Join(mongoNsSlice, "|"))
	} else if len(mongoNsSlice) == 1 {
		return fmt.Sprintf("^%s$", mongoNsSlice[0])
	} else {
		return "^$"
	}
}

func BuildFullWhereStatement(f MongoCollectionFilter) *bson.D {
	// allow commands to be watched
	allowList := append(f.Collections, MongoCollection{
		DatabaseName:   "*",
		CollectionName: "$cmd",
	})
	result := bson.D{}
	namespacesList := yslices.Map(allowList, ToRegexp)
	// noop has no collection, but should be added
	namespacesList = append(namespacesList, "")

	if len(f.Collections) > 0 {
		// if white list is empty, this filter is not needed -- all included by default
		result = append(result, bson.E{Key: "ns", Value: bson.D{{Key: "$regex", Value: buildRegex(namespacesList)}}})
	}
	if len(f.ExcludedCollections) > 0 {
		result = append(result, bson.E{Key: "ns", Value: bson.D{{Key: "$not", Value: primitive.Regex{
			Pattern: buildRegex(yslices.Map(f.ExcludedCollections, ToRegexp)),
			Options: "",
		}}}})
	}
	return &result
}

func (w localOplogRsWatcher) getFullWhereStatement() *bson.D {
	if w.fullWhereStatementCache == nil {
		w.fullWhereStatementCache = BuildFullWhereStatement(w.config.GetMongoCollectionFilter())
	}
	return w.fullWhereStatementCache
}

func (w localOplogRsWatcher) openOplogCursor(ctx context.Context, ts primitive.Timestamp) (oplogCursor *mongo.Cursor, withRegexp bool, err error) {
	openCursorFunc := func(withRegex bool) (*mongo.Cursor, bool, error) {
		var filter bson.D
		if withRegex {
			// filter out only interesting collections
			filter = *w.getFullWhereStatement()
		}
		filter = append(filter, bson.E{Key: "ts", Value: bson.D{{Key: "$gt", Value: ts}}})

		findOptions := options.Find()

		// no cursor timeout is unavailable for lower price tiers in atlas
		// https://www.mongodb.com/docs/atlas/reference/free-shared-limitations/
		findOptions.SetMaxTime(606461538 * time.Millisecond)

		cursor, err := w.oplog.Find(ctx, filter)
		return cursor, withRegexp, err
	}

	if w.filterOplogWithRegex {
		// 5k databases error:
		// "batch watching error: Couldn't open cursor for local.oplog.rs: (BadValue) Regular expression is too long"
		oplogCursor, withRegexp, err = openCursorFunc(true)
		if err != nil {
			// one of possible mongo errors: "(BadValue) Regular expression is too long"
			return oplogCursor, withRegexp, xerrors.Errorf("cannot open oplog cursor with regexp, consider turning off filtering oplog with regexp: %w", err)
		}
		return oplogCursor, withRegexp, nil
	}

	// Use case description why w.filterOplogWithRegex recommended to be 'false'
	//   Premise: there are no updates on db3, but there are updates on db1 and db2.
	//   User specifies only db3 for transferring for some reason.
	//   Then we will get no changes with this optimization, because there will be neither db1 and db2
	// changes because they are filtered nor 'noop' event because changes are happen in db1 and db2 =>
	// cluster is not idle => oplog is not empty
	// then, to cover all cases (both filter too big and emptyness of filtered oplog) we should NOT
	// use regexp filtering at all
	return openCursorFunc(false)
}

func (w localOplogRsWatcher) watchBatchFrom(ctx context.Context, ts primitive.Timestamp, batcher *keyBatcher) (until primitive.Timestamp, eventsRead int, err error) {
	w.logger.Infof("Begin watching batch from time %v", FromMongoTimestamp(ts))
	// check that timestamp is still in oplog
	from, to, err := GetLocalOplogInterval(ctx, w.client)
	if ts.Compare(from) < 0 {
		return until, eventsRead, xerrors.Errorf("local.oplog.rs watcher is out of timeline. Requested timestamp %v, actual interval: [%v, %v]",
			FromMongoTimestamp(ts),
			FromMongoTimestamp(from),
			FromMongoTimestamp(to))
	}

	oplogCursor, filterWithRegexp, err := w.openOplogCursor(ctx, ts)
	if err != nil {
		return until, eventsRead, xerrors.Errorf("couldn't open cursor for local.oplog.rs: %w", err)
	}
	defer func() {
		if err := oplogCursor.Close(ctx); err != nil {
			w.logger.Error("couldn't close oplog cursor", log.Error(err))
		}
	}()

	for oplogCursor.Next(ctx) {
		eventsRead++
		var oplogChangeEvent oplogRsChangeEventV2

		decodeTimeStart := time.Now()
		if err := oplogCursor.Decode(&oplogChangeEvent); err != nil {
			return until, eventsRead, xerrors.Errorf("Cannot decode change keyEvent: %w", err)
		}
		decodeTimeDuration := time.Since(decodeTimeStart)
		until = oplogChangeEvent.Timestamp

		mongoKeyChangeEvent, err := oplogChangeEvent.toMongoKeyChangeEvent(w.logger)
		if err != nil {
			return until, eventsRead, xerrors.Errorf("Can't convert local.oplog.rs change keyEvent: %w", err)
		}
		if mongoKeyChangeEvent == nil {
			continue // in case of no op change keyEvent
		}
		if !w.shouldReplicate(mongoKeyChangeEvent.OperationType, mongoKeyChangeEvent.Namespace) {
			if filterWithRegexp {
				// do not spam this warnings when regexp filtering is off
				w.logger.Warn("Skip document from local.oplog.rs as not belonging to replication",
					log.String("ns", mongoKeyChangeEvent.Namespace.GetFullName()),
					log.Any("oplogChangeEvent", oplogChangeEvent),
					log.Any("changeEvent", mongoKeyChangeEvent),
				)
			}
			// replace event with noop (to keep track of the oplog timeline)
			mongoKeyChangeEvent = newNoopChangeEvent(oplogChangeEvent.Timestamp)
		}
		event := &keyChangeEvent{
			keyEvent:   mongoKeyChangeEvent,
			size:       len(oplogCursor.Current),
			decodeTime: decodeTimeDuration,
		}

		if err := batcher.PushKeyChangeEvent(event); err != nil {
			return until, eventsRead, xerrors.Errorf("change keyEvent batcher error: %w", err)
		}
	}
	if oplogCursor.Err() != nil {
		return until, eventsRead, xerrors.Errorf("Cursor error: %w", oplogCursor.Err())
	}
	return until, eventsRead, err
}

func newOplogWatcher(logger log.Logger, client *MongoClientWrapper, watchFrom primitive.Timestamp,
	filterOplogWithRegexp bool,
	config *MongoSource,
) (*localOplogRsWatcher, error) {
	techDB := config.TechnicalDatabase
	if techDB == "" {
		techDB = DataTransferSystemDatabase
	}

	shouldReplicate := func(opType string, ns Namespace) bool {
		if opType == "dropDatabase" {
			// TODO(@kry127) discuss this solution
			if len(config.Collections) == 0 {
				return true // replicate all strategy
			}
			for _, coll := range config.Collections {
				if coll.DatabaseName == ns.Database {
					return true
				}
			}
		}
		if opType == "noop" {
			return true
		}
		if ns.Database == techDB {
			return false // skip replicating
		}
		return config.Include(abstract.TableID{
			Namespace: ns.Database,
			Name:      ns.Collection,
		})
	}

	oplog := client.Database("local").Collection("oplog.rs")
	watcher := &localOplogRsWatcher{
		logger:               log.With(logger, log.String("watcher", "local.oplog.rs")),
		client:               client,
		oplog:                oplog,
		filterOplogWithRegex: filterOplogWithRegexp,

		initialTime:     watchFrom,
		config:          config,
		shouldReplicate: shouldReplicate,

		fullWhereStatementCache: nil,
	}
	return watcher, nil
}
