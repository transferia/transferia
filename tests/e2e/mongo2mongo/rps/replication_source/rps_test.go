package replication

// Author: kry127
// This test check replication correct in case of intensive RPS on mongo collection
// Lifetime span of object in database is less than second, requests per minute should be approx 45MB per second

// expected statistics (25.08.2021)
// startrek:PRIMARY> db.onetimeJobs.stats()
//{
// 	"ns" : "startrek.onetimeJobs",
//	"size" : 1036123603,
//	"count" : 256612,
//	"avgObjSize" : 4037,
//	"storageSize" : 509001728,
//  ...
// }

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
	mongostorage "github.com/transferria/transferria/pkg/providers/mongo"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/e2e/mongo2mongo/rps"
	"github.com/transferria/transferria/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

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
	slotIDAkaTransferID = "dttintensiveupdatingcollection"
	DB                  = "startrek"    // tribute to StarTrek database
	Collection          = "onetimeJobs" // tribute to StarTrek collection
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = &mongostorage.MongoSource{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:     os.Getenv("MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Collections: []mongostorage.MongoCollection{
			{DatabaseName: DB, CollectionName: Collection},
		},
		SlotID: slotIDAkaTransferID,
	}
	Target = mongostorage.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("DB0_MONGO_LOCAL_PORT"),
		User:     os.Getenv("DB0_MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("DB0_MONGO_LOCAL_PASSWORD")),
		Cleanup:  model.Drop,
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

func clearSrc(t *testing.T, client *mongostorage.MongoClientWrapper) {
	t.Helper()
	var err error

	db := client.Database(DB)
	_ = db.Collection(Collection).Drop(context.Background())
	err = db.CreateCollection(context.Background(), Collection)
	require.NoError(t, err)
}

type RpsTestParameters struct {
	SrcParamGen func() *mongostorage.MongoSource
}

func RpsTest(t *testing.T) {
	Source.WithDefaults()

	for testName, testParam := range map[string]RpsTestParameters{
		"PerDatabase": {SrcParamGen: func() *mongostorage.MongoSource {
			src := *Source
			src.ReplicationSource = mongostorage.MongoReplicationSourcePerDatabase
			return &src
		}},
		"PerDatabaseFullDocument": {SrcParamGen: func() *mongostorage.MongoSource {
			src := *Source
			src.ReplicationSource = mongostorage.MongoReplicationSourcePerDatabaseFullDocument
			return &src
		}},
		"PerDatabaseUpdateDocument": {SrcParamGen: func() *mongostorage.MongoSource {
			src := *Source
			src.ReplicationSource = mongostorage.MongoReplicationSourcePerDatabaseUpdateDocument
			return &src
		}},
		"Oplog": {SrcParamGen: func() *mongostorage.MongoSource {
			src := *Source
			src.ReplicationSource = mongostorage.MongoReplicationSourceOplog
			return &src
		}},
	} {
		t.Run(testName, RpsTestFactory(testParam))
	}
}

func RpsTestFactory(testParameters RpsTestParameters) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		clientSource, err := mongostorage.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
		require.NoError(t, err)
		defer func() { _ = clientSource.Close(context.Background()) }()

		// recreate collections on source
		clearSrc(t, clientSource)
		dbSource := clientSource.Database(DB)
		collectionSource := dbSource.Collection(Collection)

		mongoSource := testParameters.SrcParamGen()
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
		rpsModel := rps.NewRpsModel(ctx, &rps.RpsCallbacks{
			OnDelete: func(ctx context.Context, key string) {
				filter := bson.D{{Key: "_id", Value: key}}
				result, err := collectionSource.DeleteOne(ctx, filter, nil)
				require.NoError(t, err)
				require.Equal(t, int64(1), result.DeletedCount)
			},
			OnCreate: func(ctx context.Context, entity rps.KV) {
				_, err := collectionSource.InsertOne(ctx, entity)
				require.NoError(t, err)
			},
			OnUpdate: func(ctx context.Context, previous rps.KV, actual rps.KV) {
				opts := options.Update()
				filter := bson.D{{Key: "_id", Value: previous.Key}}
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
			Tick: func(ctx context.Context, tickId int, model *rps.RpsModel) bool {
				normalMode := &rps.RpsSpec{
					DeleteCount:  100,
					CreateCount:  100,
					UpdateCount:  50,
					ReplaceCount: 0,
					KVConstructor: func() rps.KV {
						return rps.GenerateKV(800, 2000)
					},
					Delay: 100 * time.Millisecond,
				}
				logger.Log.Info(fmt.Sprintf("Delay iteration %d, in: %d, out: %d", tickId, len(model.Persistent), len(model.NonPersistent)))

				var currentSpec *rps.RpsSpec // when changed from nil, reconfigure happens
				message := ""
				switch tickId {
				case 0:
					message = "create initial entries"
					currentSpec = &rps.RpsSpec{
						DeleteCount:  0,
						CreateCount:  4000,
						UpdateCount:  0,
						ReplaceCount: 0,
						KVConstructor: func() rps.KV {
							return rps.GenerateKV(200, 200)
						},
						Delay: 5 * time.Second,
					}
				case 1:
					message = "then equalize insert and delete rates with normal mode"
					currentSpec = normalMode
				case 9:
					message = "make outlier in one Delay with heavy documents"
					currentSpec = &rps.RpsSpec{
						DeleteCount:  0,
						CreateCount:  40,
						UpdateCount:  20,
						ReplaceCount: 0,
						KVConstructor: func() rps.KV {
							return rps.GenerateKV(1000, 5000)
						},
						Delay: 100 * time.Millisecond,
					}
				case 10:
					message = "back to normal mode"
					currentSpec = normalMode
				case 20:
					message = "intensify update rate up to 800 requests per 10 millisecond == 80000 RPS"
					// more intensive by time pushes
					currentSpec = &rps.RpsSpec{
						DeleteCount:  200,
						CreateCount:  300,
						UpdateCount:  200,
						ReplaceCount: 100,
						KVConstructor: func() rps.KV {
							return rps.GenerateKV(300, 400)
						},
						Delay: 10 * time.Millisecond,
					}
				case 30:
					message = "maximum intensity"
					// more intensive by time pushes
					currentSpec = &rps.RpsSpec{
						DeleteCount:  500,
						CreateCount:  600,
						UpdateCount:  900,
						ReplaceCount: 300,
						KVConstructor: func() rps.KV {
							return rps.GenerateKV(50, 50)
						},
						Delay: 2 * time.Millisecond,
					}
				case 40:
					// stop generation on last Delay
					logger.Log.Info("RPS stopping", log.Int("tickId", tickId))
					return false
				}

				if currentSpec != nil {
					logger.Log.Info("RPS Reconfigure", log.String("message", message), log.Any("config", currentSpec))
					model.SetSpec(currentSpec)
				}
				return true
			},
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

		// make connection to the target
		clientTarget, err := mongostorage.Connect(ctx, Target.ConnectionOptions([]string{}), nil)
		require.NoError(t, err)
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
}
