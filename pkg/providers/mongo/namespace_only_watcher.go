//go:build !disable_mongo_provider

package mongo

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

// oneshotNamespaceRetriever subscribes for namespace changes ONLY in database
// it is used when oplog on database is no longer operational (does not goes further, i.e. BSONObjTooLarge)
// For example, use when server-side mongo cannot pack change keyEvent into single BSON because it is too large
// Assumed, that it reads the same failed keyEvent from previously failed change stream
type oneshotNamespaceRetriever struct {
	logger       log.Logger
	database     *mongo.Database
	changeStream *mongo.ChangeStream
}

// TODO can be used for skipping bad documents
func (f *oneshotNamespaceRetriever) GetResumeToken() bson.Raw {
	return f.changeStream.ResumeToken()
}

func (f *oneshotNamespaceRetriever) Close(ctx context.Context) {
	if err := f.changeStream.Close(ctx); err != nil {
		f.logger.Warn("Cannot close namespace only change stream", log.String("database", f.database.Name()), log.Error(err))
	}
}

func (f *oneshotNamespaceRetriever) GetNamespace(ctx context.Context) (*Namespace, error) {
	defer f.Close(ctx)
	if f.changeStream.Next(ctx) {
		var chgEvent ChangeEvent
		if err := f.changeStream.Decode(&chgEvent); err != nil {
			return nil, xerrors.Errorf("Cannot decode change keyEvent: %w", err)
		}

		// NOTE: if we don't handle the invalidate keyEvent, MongoDB driver stucks in an infinite loop inside changeStream.Next()
		if chgEvent.OperationType == "invalidate" {
			return nil, xerrors.Errorf(
				"Invalidate keyEvent on change stream for database %s: %v",
				f.database.Name(),
				chgEvent,
			)
		}

		return &chgEvent.Namespace, nil
	}
	return nil, f.changeStream.Err()
}

// NewOneshotNamespaceRetriever creates namespace-only watcher
// recovery point (resume token) should be specified precisely, or current time will be used
func NewOneshotNamespaceRetriever(s *mongoSource, resumeToken bson.Raw, dbName string) (*oneshotNamespaceRetriever, error) {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	db, err := s.makeDatabaseConnection(dbName)
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to database: %w", err)
	}

	// Build pipeline filter and prune all except ns
	pipeline, err := s.filter.BuildPipeline(db.Name())
	if err != nil {
		return nil, xerrors.Errorf("failed to build a filtering pipeline: %w", err)
	}
	pipeline = append(pipeline, bson.D{
		// intentionally drop out fullDocument and documentKey to fit BSONObj and read out collection name that failed to fit
		// https://docs.mongodb.com/manual/reference/change-events/
		{Key: "$project", Value: bson.D{
			{Key: "updateDescription", Value: 0},
			{Key: "fullDocument", Value: 0},
			{Key: "documentKey", Value: 0},
		}},
	})

	// Build change stream options for full document mode
	opts := &options.ChangeStreamOptions{}
	// we expect failed document to be returned
	opts.SetBatchSize(1)

	// specify exact keyEvent (or timestamp) where failing keyEvent occured
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken) // set resume token that falls immediately before failure
	} else {
		return nil, xerrors.New("you should specify resume token where watcher failed in order to retrieve timestamp")
	}

	// create watcher
	cs, err := db.Watch(s.ctx, pipeline, opts)
	rollbacks.Add(func() {
		if cs != nil {
			_ = cs.Close(s.ctx)
		}
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to start watching a database: %w", err)
	}
	if db == nil {
		return nil, xerrors.New("Mongo driver opened nil database object without error")
	}

	rollbacks.Cancel()
	return &oneshotNamespaceRetriever{
		logger:       s.logger,
		database:     db,
		changeStream: cs,
	}, nil
}
