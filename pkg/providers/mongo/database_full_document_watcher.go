//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

// namespaceOnlyWatcher subscribes for changes in database
// this is a default watcher (old behaviour)
type databaseFullDocumentWatcher struct {
	logger       log.Logger
	database     *mongo.Database
	changeStream *mongo.ChangeStream
}

func (f *databaseFullDocumentWatcher) GetResumeToken() bson.Raw {
	return f.changeStream.ResumeToken()
}

func (f *databaseFullDocumentWatcher) Close(ctx context.Context) {
	if f.changeStream != nil {
		if err := f.changeStream.Close(ctx); err != nil {
			f.logger.Warn("Cannot close change stream", log.String("database", f.database.Name()), log.Error(err))
		}
	}
}

func (f *databaseFullDocumentWatcher) Watch(ctx context.Context, pusher changeEventPusher) error {
	for f.changeStream.Next(ctx) {
		var chgEvent ChangeEvent
		st := time.Now()
		if err := f.changeStream.Decode(&chgEvent); err != nil {
			return xerrors.Errorf("Cannot decode change keyEvent: %w", err)
		}
		decodeTime := time.Since(st)

		// NOTE: if we don't handle the invalidate keyEvent, MongoDB driver stucks in an infinite loop inside changeStream.Next()
		if chgEvent.OperationType == "invalidate" {
			return xerrors.Errorf(
				"Invalidate keyEvent on full document change stream for database %s: %v",
				f.database.Name(),
				chgEvent,
			)
		}

		if err := pusher(ctx, &changeEvent{
			event:      &chgEvent,
			size:       len(f.changeStream.Current),
			decodeTime: decodeTime,
		}); err != nil {
			return xerrors.Errorf("change keyEvent pusher error : %w", err)
		}
	}

	if f.changeStream.Err() == nil {
		return xerrors.New(fmt.Sprintf("Premature end of full document change stream for database %s", f.database.Name()))
	}

	return f.changeStream.Err()
}

func (s *mongoSource) pipelineToJSON(pipeline mongo.Pipeline) string {
	canonical := true
	escapeHTML := false
	pipes := make([]string, len(pipeline))
	for i, bsonDValue := range pipeline {
		data, err := bson.MarshalExtJSON(bsonDValue, canonical, escapeHTML)
		if err != nil {
			s.logger.Warnf("Cannot marshal BSON value to JSON: %v", err)
			return ""
		}
		pipes[i] = string(data)
	}
	return fmt.Sprintf("[%v]", strings.Join(pipes, ", "))
}

func (s *mongoSource) toJSON(bsonValue interface{}) string {
	canonical := true
	escapeHTML := false
	data, err := bson.MarshalExtJSON(bsonValue, canonical, escapeHTML)
	if err != nil {
		s.logger.Warnf("Cannot marshal BSON value to JSON: %v", err)
		return ""
	}
	return string(data)
}

func NewDatabaseFullDocumentWatcher(s *mongoSource, dbPu ParallelizationUnitDatabase, rs MongoReplicationSource) (*databaseFullDocumentWatcher, error) {
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
	pipelineProject := bson.D{
		{Key: "fullDocument._id", Value: 0},
	}
	if rs != MongoReplicationSourcePerDatabaseUpdateDocument {
		// our makeChangeItem in source.go does not use update description, although it is the most probable reason of BSONObjTooLarge
		pipelineProject = append(pipelineProject, bson.E{Key: "updateDescription", Value: 0})
	}
	pipeline = append(pipeline, bson.D{
		// experimental: remove fullDocument._id from full document for space optimization
		// key is available as documentKey._id as alternative
		{Key: "$project", Value: pipelineProject},
	})

	// Build change stream options for full document mode
	opts := &options.ChangeStreamOptions{}
	if rs == MongoReplicationSourcePerDatabaseFullDocument {
		opts.SetFullDocument(options.UpdateLookup) // creates FullDocument field in change stream
	} else {
		opts.SetFullDocument(options.Default)
	}

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
	s.logger.Info("Trying to watch database", log.String("database", dbName), log.String("pipeline", s.pipelineToJSON(pipeline)), log.String("options", s.toJSON(opts)))
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
	s.logger.Infof("Change stream for %s is open", dbName)

	rollbacks.Cancel()
	return &databaseFullDocumentWatcher{
		logger:       s.logger,
		database:     db,
		changeStream: cs,
	}, nil
}
