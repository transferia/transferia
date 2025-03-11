package slots

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/randutil"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	testDB1        string = "test_db1"
	testDB2        string = "test_db2"
	testDB3        string = "test_db3"
	collectionName string = "collection"
	transferSlotID string = "dttqegn8908aata701lu"
)

var (
	allDBs = []string{testDB1, testDB2, testDB3}

	port         = helpers.GetIntFromEnv("MONGO_LOCAL_PORT")
	userName     = os.Getenv("MONGO_LOCAL_USER")
	userPassword = os.Getenv("MONGO_LOCAL_PASSWORD")
)

func getSource(collection ...mongo.MongoCollection) *mongo.MongoSource {
	return &mongo.MongoSource{
		Hosts:       []string{"localhost"},
		Port:        port,
		User:        userName,
		Password:    model.SecretString(userPassword),
		Collections: collection,
	}
}

func getTransfer(source *mongo.MongoSource) model.Transfer {
	tr := model.Transfer{
		ID:   transferSlotID,
		Type: abstract.TransferTypeSnapshotAndIncrement,
		Src:  source,
		Dst: &model.MockDestination{
			SinkerFactory: func() abstract.Sinker { return new(mockSinker) },
			Cleanup:       model.Drop,
		},
	}
	tr.FillDependentFields()
	return tr
}

func connect(source *mongo.MongoSource) (*mongo.MongoClientWrapper, error) {
	client, err := mongo.Connect(context.TODO(), source.ConnectionOptions([]string{}), nil)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func setOplogSize(ctx context.Context, client *mongo.MongoClientWrapper, sizeInSeconds, sizeInMegabytes int) error {
	hourOplogSize := float64(sizeInSeconds) / (60.0 * 60.0)

	cmdParams := bson.D{
		bson.E{Key: "replSetResizeOplog", Value: 1},
		bson.E{Key: "size", Value: float64(sizeInMegabytes)},
		bson.E{Key: "minRetentionHours", Value: hourOplogSize},
	}
	singleRes := client.Database("admin").RunCommand(ctx, cmdParams)
	if singleRes.Err() != nil {
		return singleRes.Err()
	}
	return nil
}

// just mock sinker
type mockSinker struct{}

func (m mockSinker) Close() error                           { return nil }
func (m mockSinker) Push(items []abstract.ChangeItem) error { return nil }

// controlplane that catches replication failure
type mockCPFailRepl struct {
	cpclient.CoordinatorNoOp
	err error
}

// test data structure
type Pepe struct {
	DayOfTheWeek   string
	DayOfTheWeekID int
	InsertDate     time.Time
}

func (f *mockCPFailRepl) FailReplication(transferID string, err error) error {
	f.err = err
	return nil
}

func snapshotPhase(t *testing.T, ctx context.Context, source *mongo.MongoSource) {
	sourceDBs := []string{}
	for _, coll := range source.Collections {
		sourceDBs = append(sourceDBs, coll.DatabaseName)
	}

	client, err := connect(source)
	require.NoError(t, err)
	defer client.Close(ctx)

	// prepare oplog
	// we need more than 11Mb/S RPS to exhaust oplog by time
	// note, that we need retention for 90 seconds to catch up lag (oplog flushes every 60 seconds)
	err = setOplogSize(ctx, client, 30, 990)
	require.NoError(t, err, "cannot configure oplog size")

	// drop slot info
	for _, sourceDB := range allDBs {
		_ = client.Database(sourceDB).Collection(mongo.ClusterTimeCollName).Drop(context.Background())
	}

	// insert first records
	for i, dbName := range sourceDBs {
		db := client.Database(dbName)
		_ = db.Collection(collectionName).Drop(context.Background())
		err = db.CreateCollection(context.Background(), collectionName)
		require.NoError(t, err)

		coll := db.Collection(collectionName)

		_, err = coll.InsertOne(context.Background(),
			Pepe{"Wednesday", i, time.Now()})
		require.NoError(t, err)
	}

	// start worker
	transfer := getTransfer(source)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
}

func incrementPhaseWithRestart(t *testing.T, ctx context.Context, source *mongo.MongoSource,
	updatableCollections []string, sourceAfterRestart *mongo.MongoSource) error {

	client, err := connect(source)
	require.NoError(t, err)
	defer client.Close(ctx)

	transfer := getTransfer(source)

	// start replication
	func() {
		localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
		localWorker.Start()
		defer localWorker.Stop() //nolint

		// full speed generation -- spam oplog
		timeStart := time.Now()
		for {
			for i, dbName := range updatableCollections {
				db := client.Database(dbName)
				coll := db.Collection(collectionName)

				_, err = coll.InsertOne(context.Background(),
					Pepe{randutil.GenerateString("abcdefghijklmnopqrstuvwxyz", 4*1024*1024),
						i, time.Now()})
				require.NoError(t, err)
			}

			// note: admin rights required
			oplogFromTS, _, err := mongo.GetLocalOplogInterval(ctx, client)
			require.NoError(t, err)
			if timeStart.Before(mongo.FromMongoTimestamp(oplogFromTS)) {
				// when oplog rotation happened -- terminate
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		// wait a little bit
		time.Sleep(5 * time.Second)
	}()

	// change source params (if any)
	if sourceAfterRestart != nil {
		transfer.Src = sourceAfterRestart
		transfer.FillDependentFields()
	}

	// restart replication
	newMockCP := mockCPFailRepl{}
	localWorker := local.NewLocalWorker(&newMockCP, &transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	time.Sleep(3 * time.Second)
	err = localWorker.Stop() //nolint
	require.NoError(t, err)
	require.NoError(t, localWorker.Error())
	return newMockCP.err
}

func findSlots(
	t *testing.T,
	ctx context.Context,
	source *mongo.MongoSource,
) (primitive.Timestamp, primitive.Timestamp, primitive.Timestamp) {
	client, err := connect(source)
	require.NoError(t, err)
	defer client.Close(ctx)

	// find slots for all DBs after
	var oplog1, oplog2, oplog3 primitive.Timestamp
	for dbName, oplogRef := range map[string]*primitive.Timestamp{
		testDB1: &oplog1,
		testDB2: &oplog2,
		testDB3: &oplog3,
	} {
		var pu mongo.ParallelizationUnit
		if source.ReplicationSource == mongo.MongoReplicationSourceOplog {
			pu = mongo.MakeParallelizationUnitOplog(source.TechnicalDatabase, source.SlotID)
		} else {
			pu = mongo.MakeParallelizationUnitDatabase(source.TechnicalDatabase, source.SlotID, dbName)
		}
		clusterTime, err := pu.GetClusterTime(ctx, client)
		if err == nil {
			*oplogRef = *clusterTime
		}
	}
	return oplog1, oplog2, oplog3
}

func TestMongoSlot(t *testing.T) {
	ctx := context.Background()
	t.Run("StaleDB", func(t *testing.T) {
		source := getSource(
			mongo.MongoCollection{
				DatabaseName:   testDB1,
				CollectionName: collectionName,
			},
			mongo.MongoCollection{
				DatabaseName:   testDB2,
				CollectionName: collectionName,
			},
			mongo.MongoCollection{
				DatabaseName:   testDB3,
				CollectionName: collectionName,
			})
		source.WithDefaults()

		// start snapshot phase
		snapshotPhase(t, ctx, source)

		// find slots for all DBs after activation
		oplogAfterSnapshot1, oplogAfterSnapshot2, oplogAfterSnapshot3 := findSlots(t, ctx, source)
		require.False(t, oplogAfterSnapshot1.IsZero())
		require.False(t, oplogAfterSnapshot2.IsZero())
		require.False(t, oplogAfterSnapshot3.IsZero())

		// start increment phase
		err := incrementPhaseWithRestart(t, ctx, source, []string{testDB1, testDB2}, nil)
		require.NoError(t, err)

		// find slots for all DBs after
		oplogAfterRestart1, oplogAfterRestart2, oplogAfterRestart3 := findSlots(t, ctx, source)
		require.False(t, oplogAfterRestart1.IsZero())
		require.False(t, oplogAfterRestart2.IsZero())
		require.False(t, oplogAfterRestart3.IsZero())

		// check that slots has been updated
		require.False(t, oplogAfterSnapshot1.Equal(oplogAfterRestart1), "Slot 1 should change during replication")
		require.False(t, oplogAfterSnapshot2.Equal(oplogAfterRestart2), "Slot 2 should change during replication")
		require.False(t, oplogAfterSnapshot3.Equal(oplogAfterRestart3), "Slot 3 should change during replication")

	})
	t.Run("StaleDBOplog", func(t *testing.T) {
		source := getSource(
			mongo.MongoCollection{
				DatabaseName:   testDB3,
				CollectionName: collectionName,
			})
		source.ReplicationSource = mongo.MongoReplicationSourceOplog
		source.WithDefaults()

		// start snapshot phase
		snapshotPhase(t, ctx, source)

		// find slots for all DBs after activation
		_, _, oplogAfterSnapshot3 := findSlots(t, ctx, source)
		require.False(t, oplogAfterSnapshot3.IsZero())

		// start increment phase
		err := incrementPhaseWithRestart(t, ctx, source, []string{testDB1, testDB2}, nil)
		require.NoError(t, err)

		// find slots for all DBs after
		_, _, oplogAfterRestart3 := findSlots(t, ctx, source)
		require.False(t, oplogAfterRestart3.IsZero())

		// check that slots has been updated
		require.False(t, oplogAfterSnapshot3.Equal(oplogAfterRestart3), "Slot 3 should change during replication")
	})
	// this needed for checking ChangeStreamHistoryLost
	t.Run("CheckOplogFailure", func(t *testing.T) {
		sourceBefore := getSource(mongo.MongoCollection{
			DatabaseName:   testDB1,
			CollectionName: collectionName,
		}, mongo.MongoCollection{
			DatabaseName:   testDB2,
			CollectionName: collectionName,
		})
		sourceBefore.WithDefaults()
		sourceAfter := getSource(mongo.MongoCollection{
			DatabaseName:   testDB1,
			CollectionName: collectionName,
		}, mongo.MongoCollection{
			DatabaseName:   testDB2,
			CollectionName: collectionName,
		}, mongo.MongoCollection{
			DatabaseName:   testDB3,
			CollectionName: collectionName,
		})
		sourceAfter.WithDefaults()

		// start snapshot phase
		snapshotPhase(t, ctx, sourceBefore)

		// find slots for all DBs after activation
		oplogAfterSnapshot1, oplogAfterSnapshot2, oplogAfterSnapshot3 := findSlots(t, ctx, sourceBefore)
		require.False(t, oplogAfterSnapshot1.IsZero())
		require.False(t, oplogAfterSnapshot2.IsZero())
		require.True(t, oplogAfterSnapshot3.IsZero())

		// start increment phase, and change source parameters after restart (note sourceAfter parameter)
		err := incrementPhaseWithRestart(t, ctx, sourceBefore, []string{testDB1, testDB2, testDB3}, sourceAfter)
		require.ErrorContains(t, err, "Cannot get cluster time for database 'test_db3', try to Activate transfer again.")

		// find slots for all DBs after
		oplogAfterRestart1, oplogAfterRestart2, oplogAfterRestart3 := findSlots(t, ctx, sourceAfter)
		require.False(t, oplogAfterRestart1.IsZero())
		require.False(t, oplogAfterRestart2.IsZero())
		require.True(t, oplogAfterRestart3.IsZero())

		// check that slots has been updated
		require.False(t, oplogAfterSnapshot1.Equal(oplogAfterRestart1), "Slot 1 should have change during replication")
		require.False(t, oplogAfterSnapshot2.Equal(oplogAfterRestart2), "Slot 2 should have change during replication")
	})
}
