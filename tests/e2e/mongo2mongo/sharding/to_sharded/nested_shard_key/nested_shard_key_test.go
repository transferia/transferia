package shmongo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	cpclient "github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	mongodataagent "github.com/transferria/transferria/pkg/providers/mongo"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/pkg/worker/tasks"
	mongoshardedcluster "github.com/transferria/transferria/recipe/mongo/pkg/cluster"
	"github.com/transferria/transferria/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mongo target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Ping", Ping)
		t.Run("Load", Load)
	})
}

const (
	slotIDAkaTransferID = "dtt_shard_to_shard"
	DB                  = "db1"
	Collection1         = "coll1"
	Collection2         = "coll2"
	Collection3         = "coll3"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = &mongodataagent.MongoSource{
		Hosts:      []string{os.Getenv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterHost)},
		Port:       helpers.GetIntFromEnv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterPort),
		User:       os.Getenv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterUsername),
		Password:   model.SecretString(os.Getenv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterPassword)),
		AuthSource: os.Getenv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterAuthSource),
		Collections: []mongodataagent.MongoCollection{
			{DatabaseName: DB, CollectionName: Collection1},
			{DatabaseName: DB, CollectionName: Collection2},
			{DatabaseName: DB, CollectionName: Collection3},
		},
		SlotID: slotIDAkaTransferID,
	}
	Target = mongodataagent.MongoDestination{
		Hosts:      []string{os.Getenv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterHost)},
		Port:       helpers.GetIntFromEnv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterPort),
		User:       os.Getenv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterUsername),
		Password:   model.SecretString(os.Getenv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterPassword)),
		AuthSource: os.Getenv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterAuthSource),
		Cleanup:    model.DisabledCleanup,
	}
)

func init() {
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------
// utils

func LogMongoSource(s *mongodataagent.MongoSource) {
	fmt.Printf("Source.Hosts: %v\n", s.Hosts)
	fmt.Printf("Source.Port: %v\n", s.Port)
	fmt.Printf("Source.User: %v\n", s.User)
	fmt.Printf("Source.Password: %v\n", s.Password)
}

func LogMongoDestination(s *mongodataagent.MongoDestination) {
	fmt.Printf("Target.Hosts: %v\n", s.Hosts)
	fmt.Printf("Target.Port: %v\n", s.Port)
	fmt.Printf("Target.User: %v\n", s.User)
	fmt.Printf("Target.Password: %v\n", s.Password)
}

func ShardTargetCollections(t *testing.T, client *mongodataagent.MongoClientWrapper) {
	adminDB := client.Database("admin")

	res := adminDB.RunCommand(context.TODO(),
		bson.D{
			{Key: "enableSharding", Value: DB},
		})
	require.NoError(t, res.Err())

	key1 := bson.D{
		{Key: "_id.x", Value: "hashed"},
	}

	key2 := bson.D{
		{Key: "_id.x", Value: "hashed"},
		{Key: "city", Value: 1},
	}

	key3 := bson.D{
		{Key: "_id.x", Value: "hashed"},
		{Key: "_id.y", Value: 1},
	}

	var runCmdResult bson.M
	require.NoError(t, adminDB.RunCommand(context.Background(), bson.D{
		{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", DB, Collection1)},
		{Key: "key", Value: key1},
		{Key: "unique", Value: false},
	}).Decode(&runCmdResult))

	require.NoError(t, adminDB.RunCommand(context.Background(), bson.D{
		{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", DB, Collection2)},
		{Key: "key", Value: key2},
		{Key: "unique", Value: false},
	}).Decode(&runCmdResult))

	require.NoError(t, adminDB.RunCommand(context.Background(), bson.D{
		{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", DB, Collection3)},
		{Key: "key", Value: key3},
		{Key: "unique", Value: false},
	}).Decode(&runCmdResult))
}

func Ping(t *testing.T) {
	// ping src
	LogMongoSource(Source)
	client, err := mongodataagent.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	defer func() { _ = client.Close(context.Background()) }()
	require.NoError(t, err)
	err = client.Ping(context.TODO(), nil)
	require.NoError(t, err)

	// ping dst
	LogMongoDestination(&Target)
	client2, err := mongodataagent.Connect(context.Background(), Target.ConnectionOptions([]string{}), nil)
	defer func() { _ = client2.Close(context.Background()) }()
	require.NoError(t, err)
	err = client2.Ping(context.TODO(), nil)
	require.NoError(t, err)
}

func insertOne(t *testing.T, coll *mongo.Collection, row any) {
	_, err := coll.InsertOne(context.Background(), row)
	require.NoError(t, err)
}

func updateOne(t *testing.T, coll *mongo.Collection, filter, update bson.D) {
	_, err := coll.UpdateOne(context.Background(), filter, update)
	require.NoError(t, err)
}

func Load(t *testing.T) {
	client, err := mongodataagent.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// create source collections

	db := client.Database(DB)
	err = db.CreateCollection(context.Background(), Collection1)
	require.NoError(t, err)
	err = db.CreateCollection(context.Background(), Collection2)
	require.NoError(t, err)
	err = db.CreateCollection(context.Background(), Collection3)
	require.NoError(t, err)

	coll1 := db.Collection(Collection1)
	coll2 := db.Collection(Collection2)
	coll3 := db.Collection(Collection3)

	type CompositeID struct {
		X string `bson:"x,omitempty"`
		Y string `bson:"y,omitempty"`
	}

	type Citizen struct {
		ID   CompositeID `bson:"_id"`
		Name string      `bson:"name,omitempty"`
		Age  int         `bson:"age,omitempty"`
		City string      `bson:"city,omitempty"`
	}

	jamesGordon := Citizen{
		ID:   CompositeID{"x1", "y1"},
		Name: "James Gordon",
		Age:  33,
		City: "Gotham",
	}

	insertOne(t, coll1, jamesGordon)
	insertOne(t, coll2, jamesGordon)
	insertOne(t, coll3, jamesGordon)

	//------------------------------------------------------------------------------------
	// shard target collections

	targetClient, err := mongodataagent.Connect(context.Background(), Target.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	targetDB := targetClient.Database(DB)
	err = targetDB.CreateCollection(context.Background(), Collection1)
	require.NoError(t, err)
	err = targetDB.CreateCollection(context.Background(), Collection2)
	require.NoError(t, err)
	err = targetDB.CreateCollection(context.Background(), Collection3)
	require.NoError(t, err)

	ShardTargetCollections(t, targetClient)

	//------------------------------------------------------------------------------------
	// activate

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// replicate update one record and insert one

	leslieThompkins := Citizen{
		ID:   CompositeID{"x2", "y2"},
		Name: "Leslie Thompkins",
		Age:  29,
		City: "Gotham",
	}

	insertOne(t, coll1, leslieThompkins)
	insertOne(t, coll2, leslieThompkins)
	insertOne(t, coll3, leslieThompkins)

	leslieFilter := bson.D{{Key: "_id", Value: leslieThompkins.ID}}
	leslieUpdate := bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "Atlanta"}}}}

	updateOne(t, coll1, leslieFilter, leslieUpdate)
	updateOne(t, coll2, leslieFilter, leslieUpdate)
	updateOne(t, coll3, leslieFilter, leslieUpdate)

	jamesFilter := bson.D{{Key: "_id", Value: jamesGordon.ID}}
	jamesUpdate := bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: 34}}}}

	updateOne(t, coll1, jamesFilter, jamesUpdate)
	updateOne(t, coll2, jamesFilter, jamesUpdate)
	updateOne(t, coll3, jamesFilter, jamesUpdate)

	//------------------------------------------------------------------------------------
	// check results

	require.NoError(t, helpers.WaitEqualRowsCount(t, DB, Collection1, helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, DB, Collection2, helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, DB, Collection3, helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
