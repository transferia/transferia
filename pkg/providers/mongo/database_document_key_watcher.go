//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

// databaseDocumentKeyWatcher subscribes for changes in database
// it tries to get only document id's to load changes
// using this ID's it loads documents directly
type databaseDocumentKeyWatcher struct {
	logger       log.Logger
	databaseName string
	client       *MongoClientWrapper
	changeStream *mongo.ChangeStream
	config       *MongoSource
}

func (f *databaseDocumentKeyWatcher) GetResumeToken() bson.Raw {
	return f.changeStream.ResumeToken()
}

func (f *databaseDocumentKeyWatcher) Close(ctx context.Context) {
	if err := f.changeStream.Close(ctx); err != nil {
		f.logger.Warn("Cannot close change stream", log.String("database", f.databaseName), log.Error(err))
	}
}

// Watch assumed to run in single thread
// you should call it exactly once
func (f *databaseDocumentKeyWatcher) Watch(ctx context.Context, changeEventPusher changeEventPusher) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// delegate keyEvent pushing to batcher
	batcher, err := NewKeyBatcher(ctx, f.logger, MakeDefaultFullDocumentExtractor(f.client), changeEventPusher, f.config.BatchingParams)
	if err != nil {
		return xerrors.Errorf("Cannot create batcher: %w", err)
	}
	defer func(batcher *keyBatcher) {
		if err := batcher.Close(); err != nil {
			f.logger.Error("Batcher close error: %v", log.Error(err))
		}
	}(batcher)

	for f.changeStream.Next(ctx) {
		var docIDEvent KeyChangeEvent
		st := time.Now()
		byteSize := len(f.changeStream.Current)
		if err := f.changeStream.Decode(&docIDEvent); err != nil {
			return xerrors.Errorf("Cannot decode change keyEvent: %w", err)
		}
		decodeTime := time.Since(st)

		// NOTE: if we don't handle the invalidate keyEvent, MongoDB driver stucks in an infinite loop inside changeStream.Next()
		if docIDEvent.OperationType == "invalidate" {
			return xerrors.Errorf(
				"Invalidate keyEvent on full document change stream for database %s: %v",
				f.databaseName,
				docIDEvent,
			)
		}

		err := batcher.PushKeyChangeEvent(&keyChangeEvent{
			keyEvent:   &docIDEvent,
			size:       byteSize,
			decodeTime: decodeTime,
		})
		if err != nil {
			return xerrors.Errorf("batcher push key error: %w", err)
		}
	}

	if f.changeStream.Err() != nil {
		return xerrors.Errorf("change stream error: %w", f.changeStream.Err())
	}
	return nil
}

func NewDatabaseDocumentKeyWatcher(s *mongoSource, dbPu ParallelizationUnitDatabase) (*databaseDocumentKeyWatcher, error) {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	dbName := dbPu.UnitDatabase
	db, err := s.makeDatabaseConnection(dbName)
	if err != nil {
		return nil, xerrors.Errorf("cannot connect to database: %w", err)
	}

	// Build pipeline filter for full document mode
	pipeline, err := s.filter.BuildPipeline(db.Name())
	if err != nil {
		return nil, xerrors.Errorf("cannot build pipeline: %w", err)
	}
	pipeline = append(pipeline, bson.D{
		// remove info of insert and update operations to process them separately
		{Key: "$project", Value: bson.D{
			{Key: "fullDocument", Value: 0},
			{Key: "updateDescription", Value: 0},
		}},
	})

	// Build change stream options for full document mode
	opts := &options.ChangeStreamOptions{}
	opts.SetFullDocument(options.Default) // does not create FullDocument field in change stream

	ts, err := dbPu.GetClusterTime(s.ctx, s.client)
	if err != nil {
		return nil, xerrors.Errorf("cannot get cluster time: %w", err)
	}
	if ts.IsZero() {
		s.logger.Info("Saved replication timestamp is zero, replication will proceed from the current time")
	} else {
		s.logger.Infof("Saved replication timestamp is %v", ts)
		opts.SetStartAtOperationTime(ts)
	}

	// create watcher
	cs, err := db.Watch(s.ctx, pipeline, opts)
	rollbacks.Add(func() {
		if cs != nil {
			_ = cs.Close(s.ctx)
		}
	})
	if err != nil {
		if isFatalMongoCode(err) {
			err = abstract.NewFatalError(err)
		}
		return nil, xerrors.Errorf("Cannot watch database %s: %w", dbName, err)
	}
	if db == nil {
		return nil, xerrors.New("Mongo driver opened nil database object without error")
	}

	rollbacks.Cancel()

	return &databaseDocumentKeyWatcher{
		logger:       s.logger,
		databaseName: dbName,
		client:       s.client,
		changeStream: cs,
		config:       s.config,
	}, nil
}
