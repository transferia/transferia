package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
)

type ParallelizationUnitDatabase struct {
	// technicalDatabase -- database with write permission to store technical data (e.g. slot position)
	technicalDatabase string
	// SlotID -- identifier of resource associated with replication (e.g. transfer ID)
	SlotID string
	// UnitDatabase --  what database is a replication unit of parallelizm
	UnitDatabase string
}

func (p ParallelizationUnitDatabase) String() string {
	return fmt.Sprintf("parallelization unit: {database: %v, tech_db: %v slot id: %v}", p.UnitDatabase, p.technicalDatabase, p.SlotID)
}

func (p ParallelizationUnitDatabase) systemCollectionKey() interface{} {
	if p.technicalDatabase == "" {
		return p.SlotID
	}
	return bson.D{
		bson.E{Key: "slotId", Value: p.SlotID},
		bson.E{Key: "unitDb", Value: p.UnitDatabase},
	}
}

func (p ParallelizationUnitDatabase) metadataStorageDB() string {
	if p.technicalDatabase == "" {
		return p.UnitDatabase
	}
	return p.technicalDatabase
}

func (p ParallelizationUnitDatabase) Ping(ctx context.Context, client *MongoClientWrapper) error {
	db := client.Database(p.metadataStorageDB())
	clusterTimeColl := db.Collection(ClusterTimeCollName)
	opts := mongo_options.Update().SetUpsert(true)
	_, err := clusterTimeColl.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: p.systemCollectionKey()}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "worker_time", Value: time.Now()}}},
		},
		opts,
	)
	return err
}

func (p ParallelizationUnitDatabase) SaveClusterTime(ctx context.Context, client *MongoClientWrapper, timestamp *primitive.Timestamp) error {
	db := client.Database(p.metadataStorageDB())
	clusterTimeColl := db.Collection(ClusterTimeCollName)
	opts := mongo_options.Update().SetUpsert(true)
	if _, err := clusterTimeColl.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: p.systemCollectionKey()}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "cluster_time", Value: *timestamp}}},
		},
		opts,
	); err != nil {
		return xerrors.Errorf("cannot update cluster time: %w", err)
	}
	return nil
}

func (p ParallelizationUnitDatabase) GetClusterTime(ctx context.Context, client *MongoClientWrapper) (*primitive.Timestamp, error) {
	ts, err := p.getClusterTime(ctx, client)
	if err != nil {
		return nil, xerrors.Errorf(
			"Cannot get cluster time for database '%s', try to Activate transfer again. Technical database: %s, Slot ID: %s. Reason: %w",
			p.UnitDatabase, p.technicalDatabase, p.SlotID, err,
		)
	}
	return ts, nil
}

func (p ParallelizationUnitDatabase) getClusterTime(ctx context.Context, client *MongoClientWrapper) (*primitive.Timestamp, error) {
	var tr TimeCollectionScheme

	db := client.Database(p.metadataStorageDB())
	clusterTimeColl := db.Collection(ClusterTimeCollName)
	findOneResult := clusterTimeColl.FindOne(ctx, bson.D{{Key: "_id", Value: p.systemCollectionKey()}})
	if err := findOneResult.Err(); err != nil {
		if mongo_driver.IsNetworkError(err) {
			return nil, xerrors.Errorf("network error: %w", err)
		}
		return nil, abstract.NewFatalError(xerrors.Errorf("Cannot fetch cluster time: %w", err))
	}
	if err := findOneResult.Decode(&tr); err != nil {
		return nil, abstract.NewFatalError(xerrors.Errorf("Cannot decode cluster time: %w", err))
	}
	result := tr.Time
	return &result, nil

}

func MakeParallelizationUnitDatabase(technicalDatabase, slotID, dbName string) ParallelizationUnitDatabase {
	return ParallelizationUnitDatabase{
		technicalDatabase: technicalDatabase,
		SlotID:            slotID,
		UnitDatabase:      dbName,
	}
}
