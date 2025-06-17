//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ParallelizationUnitOplog struct {
	// technicalDatabase -- database with write permission to store technical data (e.g. slot position)
	technicalDatabase string
	// SlotID -- identifier of resource associated with replication (e.g. transfer ID)
	SlotID string
}

func (p ParallelizationUnitOplog) metadataStorageDB() string {
	if p.technicalDatabase == "" {
		return DataTransferSystemDatabase
	}
	return p.technicalDatabase
}

func (p ParallelizationUnitOplog) String() string {
	return fmt.Sprintf("parallelization unit: {replication oplog local.oplog.rs, slot id: %v, technicalDatabase: %v}", p.SlotID, p.technicalDatabase)
}

func (p ParallelizationUnitOplog) Ping(ctx context.Context, client *MongoClientWrapper) error {
	db := client.Database(p.metadataStorageDB())
	clusterTimeColl := db.Collection(ClusterTimeCollName)
	opts := options.Update().SetUpsert(true)
	_, err := clusterTimeColl.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: p.SlotID}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "worker_time", Value: time.Now()}}},
		},
		opts,
	)
	return err
}

func (p ParallelizationUnitOplog) SaveClusterTime(ctx context.Context, client *MongoClientWrapper, timestamp *primitive.Timestamp) error {
	db := client.Database(p.metadataStorageDB())
	clusterTimeColl := db.Collection(ClusterTimeCollName)
	opts := options.Update().SetUpsert(true)
	_, err := clusterTimeColl.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: p.SlotID}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "cluster_time", Value: *timestamp}}},
		},
		opts,
	)
	return err
}

func (p ParallelizationUnitOplog) GetClusterTime(ctx context.Context, client *MongoClientWrapper) (*primitive.Timestamp, error) {
	ts, err := p.getClusterTime(ctx, client)
	if err != nil {
		clusterTimeErr := xerrors.Errorf(
			"Cannot get cluster time for local.oplog.rs, try to Activate transfer again. Slot ID: %s. Reason: %w",
			p.SlotID, err,
		)
		return nil, abstract.NewFatalError(clusterTimeErr)
	}
	return ts, nil
}

func (p ParallelizationUnitOplog) getClusterTime(ctx context.Context, client *MongoClientWrapper) (*primitive.Timestamp, error) {
	var tr TimeCollectionScheme

	db := client.Database(p.metadataStorageDB())
	clusterTimeColl := db.Collection(ClusterTimeCollName)
	findOneResult := clusterTimeColl.FindOne(ctx, bson.D{{Key: "_id", Value: p.SlotID}})
	if findOneResult.Err() != nil {
		return nil, xerrors.Errorf("Cannot fetch cluster time: %w", findOneResult.Err())
	}
	if err := findOneResult.Decode(&tr); err != nil {
		return nil, xerrors.Errorf("Cannot decode cluster time: %w", err)
	}
	result := tr.Time
	return &result, nil
}

func MakeParallelizationUnitOplog(technicalDatabase, slotID string) ParallelizationUnitOplog {
	return ParallelizationUnitOplog{
		technicalDatabase: technicalDatabase,
		SlotID:            slotID,
	}
}
