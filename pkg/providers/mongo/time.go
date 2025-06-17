//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/set"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TimeCollectionScheme struct {
	Time       primitive.Timestamp `bson:"cluster_time"`
	WorkerTime time.Time           `bson:"worker_time"`
}

func ToMongoTimestamp(t time.Time) primitive.Timestamp {
	return primitive.Timestamp{
		T: uint32(t.Unix()),
		I: 0,
	}
}

func FromMongoTimestamp(t primitive.Timestamp) time.Time {
	return time.Unix(int64(t.T), 0)
}

func GetLocalOplogInterval(ctx context.Context, client *MongoClientWrapper) (from, to primitive.Timestamp, _err error) {
	type TimestampHolder struct {
		Timestamp primitive.Timestamp `bson:"ts"`
	}
	var fromTimestampHolder, toTimestampHolder TimestampHolder
	localDB := client.Database("local")
	ol := localDB.Collection("oplog.rs")

	findFromOptions, findToOptions := options.FindOne(), options.FindOne()
	findFromOptions.SetSort(bson.D{{Key: "$natural", Value: 1}})
	findToOptions.SetSort(bson.D{{Key: "$natural", Value: -1}})
	if err := ol.FindOne(ctx, bson.D{}, findFromOptions).Decode(&fromTimestampHolder); err != nil {
		return from, to, xerrors.Errorf("error finding left oplog timestamp: %w", err)
	}
	if err := ol.FindOne(ctx, bson.D{}, findToOptions).Decode(&toTimestampHolder); err != nil {
		return from, to, xerrors.Errorf("error finding right oplog timestamp: %w", err)
	}

	return fromTimestampHolder.Timestamp, toTimestampHolder.Timestamp, nil
}

// SyncClusterTime is mongo version dependent code
func SyncClusterTime(ctx context.Context, src *MongoSource, defaultCACertPaths []string) error {
	client, err := Connect(ctx, src.ConnectionOptions(defaultCACertPaths), nil)
	if err != nil {
		return xerrors.New(fmt.Sprintf("Cannot connect to MongoDB at %v:%v: %v", src.Hosts, src.Port, err))
	}
	defer func(client *MongoClientWrapper, ctx context.Context) {
		_ = client.Close(ctx)
	}(client, ctx)

	// version-dependent code
	mongoVersion, err := GetVersion(ctx, client, src.AuthSource)
	if err != nil {
		return xerrors.Errorf("cannot get mongo version: %w", err)
	}

	replicationSource := ReplicationSourceFallback(logger.Log, src.ReplicationSource, mongoVersion)

	switch replicationSource {
	case MongoReplicationSourceUnspecified:
		fallthrough
	case MongoReplicationSourcePerDatabase, MongoReplicationSourcePerDatabaseFullDocument, MongoReplicationSourcePerDatabaseUpdateDocument:
		if err := syncPerDatabaseClusterTime(ctx, src, client); err != nil {
			return xerrors.Errorf("Couldn't set cluster time for mongo 4.0+ version: %w", err)
		}
	case MongoReplicationSourceOplog:
		if err := syncOplogClusterTime(ctx, src, client); err != nil {
			return xerrors.Errorf("Couldn't set cluster time for mongo 3.4+ version: %w", err)
		}
	default:
		return xerrors.Errorf("Unknown replication source: %s", string(replicationSource))
	}

	return nil
}

func syncOplogClusterTime(ctx context.Context, src *MongoSource, client *MongoClientWrapper) error {
	oplogPu := MakeParallelizationUnitOplog(src.TechnicalDatabase, src.SlotID)
	if err := setClusterTimeForOplog(ctx, client, oplogPu); err != nil {
		return xerrors.Errorf("unable to set cluster time for oplog: %w", err)
	}
	return nil
}

func syncPerDatabaseClusterTime(ctx context.Context, src *MongoSource, client *MongoClientWrapper) error {
	var colls []MongoCollection
	if len(src.Collections) == 0 {
		allColls, err := GetAllExistingCollections(ctx, client)
		if err != nil {
			return xerrors.Errorf("couldn't get all collection list: %w", err)
		}
		colls = allColls
	} else {
		colls = src.Collections
	}
	if client.IsDocDB {
		if err := EnableChangeStreams(ctx, client, colls); err != nil {
			return xerrors.Errorf("unable to init docdb change streams: %w", err)
		}
	}

	dbs := map[string]struct{}{}
	var databaseParallelizationUnits []ParallelizationUnitDatabase
	for _, col := range colls {
		if _, exists := dbs[col.DatabaseName]; !exists {
			dbs[col.DatabaseName] = struct{}{}
			databaseParallelizationUnits = append(databaseParallelizationUnits,
				MakeParallelizationUnitDatabase(src.TechnicalDatabase, src.SlotID, col.DatabaseName))
		}
	}
	for _, parallelizationUnit := range databaseParallelizationUnits {
		if err := setClusterTimeForDatabase(ctx, client, parallelizationUnit); err != nil {
			return xerrors.Errorf("unable to set cluster time for database %s: %w", parallelizationUnit.UnitDatabase, err)
		}
	}
	return nil
}

func EnableChangeStreams(ctx context.Context, client *MongoClientWrapper, colls []MongoCollection) error {
	dbs := set.New[string]()
	for _, coll := range colls {
		dbs.Add(coll.DatabaseName)
		var commandResult bson.D
		command := bson.D{
			{Key: "modifyChangeStreams", Value: 1},
			{Key: "database", Value: coll.DatabaseName},
			{Key: "collection", Value: coll.CollectionName},
			{Key: "enable", Value: true},
		}
		// The modifyChangeStreams command can only be run against the admin database
		if err := client.Database("admin").RunCommand(ctx, command).Decode(&commandResult); err != nil {
			return xerrors.Errorf("error getting enabling change stream: %w", err)
		}
		logger.Log.Infof("enable change stream for: %s", coll.String())
	}
	for _, db := range dbs.Slice() {
		var commandResult bson.D
		command := bson.D{
			{Key: "modifyChangeStreams", Value: 1},
			{Key: "database", Value: db},
			{Key: "collection", Value: ClusterTimeCollName},
			{Key: "enable", Value: true},
		}
		if err := client.Database("admin").RunCommand(ctx, command).Decode(&commandResult); err != nil {
			return xerrors.Errorf("error getting enabling change stream: %w", err)
		}
		logger.Log.Infof("enable change stream for system table: %s at %s database", ClusterTimeCollName, db)
	}
	return nil
}

func setClusterTimeForOplog(ctx context.Context, client *MongoClientWrapper, oplogPu ParallelizationUnitOplog) error {
	_, to, err := GetLocalOplogInterval(ctx, client)
	if err != nil {
		return xerrors.Errorf("Cannot get oplog timestamp interval: %w", err)
	}

	if err := oplogPu.SaveClusterTime(ctx, client, &to); err != nil {
		return xerrors.Errorf("error writing timestamp: %w", err)
	}
	return nil
}

func setClusterTimeForDatabase(ctx context.Context, client *MongoClientWrapper, dbPu ParallelizationUnitDatabase) error {
	dbName := dbPu.metadataStorageDB()
	changeStream, err := client.Database(dbName).Watch(ctx, mongo.Pipeline{})
	if err != nil {
		return xerrors.Errorf("Cannot watch %s database: %v", dbName, err)
	}
	defer func(changeStream *mongo.ChangeStream, ctx context.Context) {
		_ = changeStream.Close(ctx)
	}(changeStream, ctx)

	if err := dbPu.Ping(ctx, client); err != nil {
		return err
	}
	if ok := changeStream.Next(ctx); !ok {
		return xerrors.New(fmt.Sprintf("Cannot read event from %s database change stream: %v", dbName, changeStream.Err()))
	}
	var event ChangeEvent
	if err = changeStream.Decode(&event); err != nil {
		return xerrors.New(fmt.Sprintf("Cannot decode event from %s database change stream: %v", dbName, err))
	}
	if event.ClusterTime.T == 0 {
		return xerrors.Errorf(
			"Cannot decode event from %s database change stream: clusterTime is zero. MongoDB version is probably older than 4.0",
			dbName,
		)
	}

	if err = dbPu.SaveClusterTime(ctx, client, &event.ClusterTime); err != nil {
		return xerrors.New(fmt.Sprintf("Cannot update %s database: %v", dbName, err))
	}
	return nil
}
