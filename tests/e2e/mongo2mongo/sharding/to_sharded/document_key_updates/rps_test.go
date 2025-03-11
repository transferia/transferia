package shmongo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	mongostorage "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/randutil"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	mongoshardedcluster "github.com/transferia/transferia/recipe/mongo/pkg/cluster"
	"github.com/transferia/transferia/tests/e2e/mongo2mongo/rps"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestGroup(t *testing.T) {
	t.Skip("TM-5255 temporary skip tests")

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mongo target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Ping", Ping)
		t.Run("RpsTest", RpsTest)
	})
}

const (
	slotIDAkaTransferID = "dtt_shard_to_shard"
	DB                  = "db1"
	Collection          = "coll1"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = &mongostorage.MongoSource{
		Hosts:      []string{os.Getenv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterHost)},
		Port:       helpers.GetIntFromEnv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterPort),
		User:       os.Getenv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterUsername),
		Password:   model.SecretString(os.Getenv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterPassword)),
		AuthSource: os.Getenv("DB1_" + mongoshardedcluster.EnvMongoShardedClusterAuthSource),
		Collections: []mongostorage.MongoCollection{
			{DatabaseName: DB, CollectionName: Collection},
		},
		SlotID: slotIDAkaTransferID,
	}
	Target = mongostorage.MongoDestination{
		Hosts:      []string{os.Getenv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterHost)},
		Port:       helpers.GetIntFromEnv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterPort),
		User:       os.Getenv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterUsername),
		Password:   model.SecretString(os.Getenv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterPassword)),
		AuthSource: os.Getenv("DB2_" + mongoshardedcluster.EnvMongoShardedClusterAuthSource),
		Cleanup:    model.DisabledCleanup,
	}
)

//---------------------------------------------------------------------------------------------------------------------
// utils

func LogMongoSource(s *mongostorage.MongoSource) {
	fmt.Printf("Source.Hosts: %v\n", s.Hosts)
	fmt.Printf("Source.Port: %v\n", s.Port)
	fmt.Printf("Source.User: %v\n", s.User)
	fmt.Printf("Source.Password: %v\n", s.Password)
}

func LogMongoDestination(s *mongostorage.MongoDestination) {
	fmt.Printf("Target.Hosts: %v\n", s.Hosts)
	fmt.Printf("Target.Port: %v\n", s.Port)
	fmt.Printf("Target.User: %v\n", s.User)
	fmt.Printf("Target.Password: %v\n", s.Password)
}

func MakeDstClient(t *mongostorage.MongoDestination) (*mongostorage.MongoClientWrapper, error) {
	return mongostorage.Connect(context.Background(), t.ConnectionOptions([]string{}), nil)
}

func ShardSourceCollection(t *testing.T, client *mongostorage.MongoClientWrapper) {
	adminDB := client.Database("admin")

	res := adminDB.RunCommand(context.TODO(),
		bson.D{
			{Key: "enableSharding", Value: DB},
		})
	require.NoError(t, res.Err())

	var runCmdResult bson.M
	require.NoError(t, adminDB.RunCommand(context.Background(), bson.D{
		{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", DB, Collection)},
		{Key: "key", Value: bson.D{
			{Key: "document.a", Value: "hashed"},
			{Key: "document.b", Value: 1},
			{Key: "document.c", Value: 1},
		}},
		{Key: "unique", Value: false},
	}).Decode(&runCmdResult))
}

func ShardTargetCollection(t *testing.T, client *mongostorage.MongoClientWrapper) {
	adminDB := client.Database("admin")

	res := adminDB.RunCommand(context.TODO(),
		bson.D{
			{Key: "enableSharding", Value: DB},
		})
	require.NoError(t, res.Err())

	key := bson.D{
		{Key: "document.x", Value: "hashed"},
		{Key: "document.y", Value: 1},
		{Key: "document.z", Value: 1},
	}

	var runCmdResult bson.M
	require.NoError(t, adminDB.RunCommand(context.Background(), bson.D{
		{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", DB, Collection)},
		{Key: "key", Value: key},
		{Key: "unique", Value: false},
	}).Decode(&runCmdResult))
}

func Ping(t *testing.T) {
	// ping src
	LogMongoSource(Source)
	client, err := mongostorage.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	defer func() { _ = client.Close(context.Background()) }()
	require.NoError(t, err)
	err = client.Ping(context.TODO(), nil)
	require.NoError(t, err)

	// ping dst
	LogMongoDestination(&Target)
	client2, err := MakeDstClient(&Target)
	defer func() { _ = client2.Close(context.Background()) }()
	require.NoError(t, err)
	err = client2.Ping(context.TODO(), nil)
	require.NoError(t, err)
}

func clearStorage(t *testing.T, client *mongostorage.MongoClientWrapper) {
	t.Helper()
	var err error

	db := client.Database(DB)
	_ = db.Collection(Collection).Drop(context.Background())
	err = db.CreateCollection(context.Background(), Collection)
	require.NoError(t, err)
}

func RpsTest(t *testing.T) {
	for _, rsName := range []mongostorage.MongoReplicationSource{
		mongostorage.MongoReplicationSourcePerDatabaseFullDocument,
		mongostorage.MongoReplicationSourcePerDatabaseUpdateDocument,
	} {
		t.Run(string(rsName), func(t *testing.T) {
			RpsTestForRS(t, rsName)
		})
	}
}

func RpsTestForRS(t *testing.T, rs mongostorage.MongoReplicationSource) {
	ctx := context.Background()

	clientSource, err := mongostorage.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = clientSource.Close(context.Background()) }()

	// recreate collections on source
	clearStorage(t, clientSource)
	dbSource := clientSource.Database(DB)
	collectionSource := dbSource.Collection(Collection)

	// make connection to the target
	clientTarget, err := mongostorage.Connect(ctx, Target.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	// drop collection on target before sharding
	clearStorage(t, clientTarget)

	// shard source
	//ShardSourceCollection(t, clientSource) //IS it recipe limitation?
	// shard target
	ShardTargetCollection(t, clientTarget)

	mongoSource := Source
	mongoSource.ReplicationSource = rs
	transfer := helpers.MakeTransfer(helpers.TransferID, mongoSource, &Target, TransferType)

	// activate transfer
	err = tasks.ActivateDelivery(ctx, nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	// start local worker for activation
	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	errChan := make(chan error, 1)
	go func() {
		errChan <- localWorker.Run() // like .Start(), but we in control for processing error in test
	}()

	dstStorage, err := mongostorage.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)

	// configure desired RPS
	rpsContext, rpsCancel := context.WithCancel(ctx)
	defer rpsCancel()
	rpsModel := rps.NewRpsModel(rpsContext, &rps.RpsCallbacks{
		OnCreate: func(ctx context.Context, entity rps.KV) {
			_, err := collectionSource.InsertOne(ctx, entity)
			require.NoError(t, err)
		},
		OnUpdate: func(ctx context.Context, previous rps.KV, actual rps.KV) {
			opts := options.Update()
			doc, ok := previous.Document.(bson.D)
			require.True(t, ok)
			filter := bson.D{
				{Key: "_id", Value: previous.Key},
				{Key: "document.a", Value: doc.Map()["a"]},
				{Key: "document.b", Value: doc.Map()["b"]},
				{Key: "document.c", Value: doc.Map()["c"]},
			}
			update := bson.D{{Key: "$set", Value: bson.D{{Key: "document", Value: actual.Document}}}}
			result, err := collectionSource.UpdateOne(ctx, filter, update, opts)
			require.NoError(t, err)
			require.Equal(t, int64(1), result.ModifiedCount)
		},
		OnReplace: func(ctx context.Context, previous rps.KV, actual rps.KV) {
			opts := options.Replace()
			filter := bson.D{{Key: "_id", Value: previous.Key}}
			result, err := collectionSource.ReplaceOne(ctx, filter, actual, opts)
			require.NoError(t, err)
			require.Equal(t, int64(1), result.ModifiedCount)
		},
		OnDelete: func(ctx context.Context, key string) {
			filter := bson.D{{Key: "_id", Value: key}}
			result, err := collectionSource.DeleteOne(ctx, filter, nil)
			require.NoError(t, err)
			require.Equal(t, int64(1), result.DeletedCount)
		},
		Tick: func(ctx context.Context, tickId int, model *rps.RpsModel) bool {
			if tickId > 12 {
				// stop generation on last Delay
				logger.Log.Info("RPS stopping", log.Int("tickId", tickId))
				return false
			}
			return true
		},
	})

	rpsModel.SetSpec(&rps.RpsSpec{
		DeleteCount:  100,
		CreateCount:  100,
		UpdateCount:  100,
		ReplaceCount: 100,
		KVConstructor: func() rps.KV {
			return rps.KV{
				Key: randutil.GenerateAlphanumericString(16),
				Document: bson.D{
					{Key: "a", Value: randutil.GenerateAlphanumericString(8)},
					{Key: "b", Value: randutil.GenerateAlphanumericString(8)},
					{Key: "c", Value: randutil.GenerateAlphanumericString(8)},
					{Key: "x", Value: randutil.GenerateAlphanumericString(8)},
					{Key: "y", Value: randutil.GenerateAlphanumericString(8)},
					{Key: "z", Value: randutil.GenerateAlphanumericString(8)},
				},
			}
		},
		Delay: 0,
	})

	logger.Log.Info("Start RPS generator")
	rpsModelDone := make(chan struct{})
	go func() {
		defer rpsModel.Close()
		defer close(rpsModelDone)
		rpsModel.Start()
	}()
	select {
	case <-rpsModelDone:
		break
	case <-ctx.Done():
		t.Fatal("Couldn't wait for RPS to close")
	}

	// wait for replication to catch up lag
	rowCount := uint64(len(rpsModel.Persistent))
	tryingsCount := 30
	tries := 0
	for tries = 0; tries < tryingsCount; tries++ {
		td := abstract.TableID{Namespace: DB, Name: Collection}
		dstTableSize, err := dstStorage.ExactTableRowsCount(td) // TODO(@kry127;@timmyb32r) TM2409 change on GetRowsCount()
		require.NoError(t, err)

		t.Logf("Table: %s, count rows. Expected: %d, actual: %d", td.Fqtn(), rowCount, dstTableSize)
		if dstTableSize == rowCount {
			break
		}
		time.Sleep(time.Second)
	}
	if tries == tryingsCount {
		// nevermind, further test is unpassable
		t.Logf("Tries are over: %d out of %d", tries, tryingsCount)
	}

	// wait a little bit (push batch delay is recomended)
	time.Sleep(3 * mongostorage.DefaultBatchFlushInterval)

	// stop worker
	logger.Log.Info("Stop local worker")
	err = localWorker.Stop()
	require.NoError(t, err)

	// wait for appropriate error from replication
	select {
	case err := <-errChan:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatalf("Couldn't wait until replication ended: %v", ctx.Err())
	}

	dbTarget := clientTarget.Database(DB)
	collectionTarget := dbTarget.Collection(Collection)

	// check that 'persistent' is present in source and target, and they values are equal
	// and check that 'not persistent' neither on source nor target
	logger.Log.Info("Validation of source and target databases")
	for fromWhere, coll := range map[string]*mongo.Collection{"source": collectionSource, "target": collectionTarget} {
		rpsModel.CheckValid(t, ctx, fromWhere, coll)
	}

	logger.Log.Info("All values validated, tear down")
}
