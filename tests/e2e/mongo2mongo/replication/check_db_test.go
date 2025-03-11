package snapshot

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	cpclient "github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	mongodataagent "github.com/transferria/transferria/pkg/providers/mongo"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = mongodataagent.MongoSource{
		Hosts:       []string{"localhost"},
		Port:        helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:        os.Getenv("MONGO_LOCAL_USER"),
		Password:    model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Collections: []mongodataagent.MongoCollection{{DatabaseName: "db", CollectionName: "timmyb32r_test"}},
	}
	Target = mongodataagent.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("DB0_MONGO_LOCAL_PORT"),
		User:     os.Getenv("DB0_MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("DB0_MONGO_LOCAL_PASSWORD")),
		Cleanup:  model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
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

func MakeDstClient(t *mongodataagent.MongoDestination) (*mongodataagent.MongoClientWrapper, error) {
	return mongodataagent.Connect(context.Background(), t.ConnectionOptions([]string{}), nil)
}

//---------------------------------------------------------------------------------------------------------------------

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
		t.Run("ReplicationShutdownTest", ReplicationShutdownTest)
		t.Run("ReplicationOfDropDatabaseTest", ReplicationOfDropDatabaseTest)
	})
}

func Ping(t *testing.T) {
	// ping src
	LogMongoSource(&Source)
	client, err := mongodataagent.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
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

func Load(t *testing.T) {
	client, err := mongodataagent.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	//------------------------------------------------------------------------------------
	// insert one record

	db := client.Database("db")
	defer func() {
		// clear collection in the end (for local debug)
		_ = db.Collection("timmyb32r_test").Drop(context.Background())
	}()
	err = db.CreateCollection(context.Background(), "timmyb32r_test")
	require.NoError(t, err)

	coll := db.Collection("timmyb32r_test")

	type Trainer struct {
		Name string
		Age  int
		City string
	}

	_, err = coll.InsertOne(context.Background(), Trainer{"a", 1, "aa"})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := model.Transfer{
		Type: abstract.TransferTypeSnapshotAndIncrement,
		Src:  &Source,
		Dst:  &Target,
		ID:   helpers.TransferID,
	}

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// replicate one record

	_, err = coll.InsertOne(context.Background(), Trainer{"b", 2, "bb"})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// check results

	require.NoError(t, helpers.WaitEqualRowsCount(t, "db", "timmyb32r_test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

// define mock sinker to report error
var MockSinkerError = xerrors.New("You picked the wrong house, fool!")

type mockSinker struct {
	batchesTillErr int
}

func newMockSinker(batchesTillErr int) *mockSinker {
	return &mockSinker{
		batchesTillErr: batchesTillErr,
	}
}

func (m *mockSinker) dec() {
	m.batchesTillErr--
}

func (m *mockSinker) Close() error {
	return nil
}

func (m *mockSinker) Push(input []abstract.ChangeItem) error {
	defer m.dec()
	if m.batchesTillErr <= 1 {
		return MockSinkerError
	}
	return nil
}

func ReplicationShutdownTest(t *testing.T) {
	ctx := context.Background()

	logger.Log.Info("Connect to mongo source database")
	clientSource, err := mongodataagent.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = clientSource.Close(context.Background()) }()

	type Parquet struct{ X, Y int }

	logger.Log.Info("Prepare mongo source database")
	db, collection := "db100500", "shutdowntest"
	dbSource := clientSource.Database(db)
	collectionSource := dbSource.Collection(collection)
	defer func() {
		_ = collectionSource.Drop(context.Background())
	}()
	_, err = collectionSource.InsertOne(context.Background(), Parquet{X: 5, Y: 10})
	require.NoError(t, err)

	slotID := "shutdowntransfer"
	logger.Log.Info("Specify replication parameters")
	source := Source
	source.Collections = []mongodataagent.MongoCollection{{DatabaseName: db, CollectionName: collection}}
	source.SlotID = slotID
	transfer := model.Transfer{
		Type: abstract.TransferTypeIncrementOnly,
		Src:  &source,
		Dst: &model.MockDestination{
			SinkerFactory: func() abstract.Sinker {
				return newMockSinker(3)
			},
		},
		ID: slotID,
	}

	logger.Log.Info("Activate transfer")
	err = tasks.ActivateDelivery(ctx, nil, cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	logger.Log.Info("Start local worker for activation")
	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	errChan := make(chan error, 1)
	var waitForLocalWorker sync.WaitGroup
	waitForLocalWorker.Add(1)
	go func() {
		waitForLocalWorker.Done()
		errChan <- localWorker.Run() // like .Start(), but we in control for processing error in test
	}()
	logger.Log.Info("Wait for worker to start")
	waitForLocalWorker.Wait()

	logger.Log.Info("Add some documents to make replication busy")
	for round := 0; round < 20; round++ {
		select {
		case err := <-errChan:
			require.ErrorIs(t, err, MockSinkerError)
			return
		default:
		}
		parquetAmount := 3
		var parquetList []interface{}
		for i := 0; i < parquetAmount; i++ {
			parquetList = append(parquetList, Parquet{X: 2 * (33 + parquetAmount - i), Y: 10 * i})
		}
		insertManyRes, err := collectionSource.InsertMany(context.Background(), parquetList)
		require.NoError(t, err)
		require.Equal(t, len(insertManyRes.InsertedIDs), parquetAmount, "Amount of inserted documents didn't match requested amount")
		time.Sleep(1 * time.Second) // every 1 second sinker accepts new batch
	}

	logger.Log.Info("Wait for appropriate error on replication")
	tmr := time.NewTimer(5 * time.Second)
	select {
	case err := <-errChan:
		require.ErrorIs(t, err, MockSinkerError)
	case <-tmr.C:
		logger.Log.Error("Too long no shutdown! Replication hanged on deadlock (possibly)")
		t.Fail()
	}
}

func ReplicationOfDropDatabaseTest(t *testing.T) {
	t.Run("PerDatabase", func(t *testing.T) {
		ReplicationOfDropDatabaseFromReplSourceTest(t, mongodataagent.MongoReplicationSourcePerDatabase)
	})
	t.Run("Oplog", func(t *testing.T) {
		ReplicationOfDropDatabaseFromReplSourceTest(t, mongodataagent.MongoReplicationSourceOplog)
	})
}

func ReplicationOfDropDatabaseFromReplSourceTest(t *testing.T, replSource mongodataagent.MongoReplicationSource) {
	logger.Log.Info("Checking that dropping collection in source is replicated in target")
	logger.Log.Infof("Replication source: %s", replSource)
	ctx := context.Background()

	logger.Log.Info("Connect to mongo source database")
	clientSource, err := mongodataagent.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = clientSource.Close(context.Background()) }()

	logger.Log.Info("Connect to mongo target database")
	clientTarget, err := MakeDstClient(&Target)
	require.NoError(t, err)
	defer func() { _ = clientTarget.Close(context.Background()) }()

	logger.Log.Info("Prepare mongo source database")
	db, collection := "db_that_will_die", "shutdowntest"
	dbSource := clientSource.Database(db)
	dbTarget := clientTarget.Database(db)
	collectionSource := dbSource.Collection(collection)
	defer func() {
		_ = collectionSource.Drop(context.Background())
	}()
	logger.Log.Infof("Drop database '%s' on target if exists before test", db)
	err = dbTarget.Drop(context.Background())
	require.NoError(t, err)

	logger.Log.Infof("Insert document in database '%s' collection '%s' in order to create db and subscribe for changes", db, collection)
	_, err = collectionSource.InsertOne(context.Background(), struct{ Val int }{Val: 9})
	require.NoError(t, err)

	srcList, err := clientSource.ListDatabaseNames(ctx, bson.D{{Key: "name", Value: db}})
	require.NoError(t, err)
	require.Len(t, srcList, 1, "Database should exist before replication start")

	slotID := "dropdatabase"
	logger.Log.Info("Specify replication parameters")
	source := Source
	source.Collections = []mongodataagent.MongoCollection{{DatabaseName: db, CollectionName: collection}}
	source.SlotID = slotID
	source.ReplicationSource = replSource
	transfer := model.Transfer{
		Type: abstract.TransferTypeIncrementOnly,
		Src:  &source,
		Dst:  &Target,
		ID:   slotID,
	}

	logger.Log.Info("Activate transfer")
	err = tasks.ActivateDelivery(ctx, nil, cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	logger.Log.Info("Start local worker for activation")
	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer func(localWorker *local.LocalWorker) {
		_ = localWorker.Stop()
	}(localWorker)

	time.Sleep(2 * time.Second)

	logger.Log.Infof("Insert another document in database '%s' collection '%s'", db, collection)
	_, err = collectionSource.InsertOne(context.Background(), struct{ Val int }{Val: 1})
	require.NoError(t, err)

	logger.Log.Info("Wait for db creation on target")
	for retryCount, maxRetryCount := 1, 14; retryCount <= maxRetryCount; retryCount++ {
		logger.Log.Infof("Attempt %d of %d for database '%s' to appear", retryCount, maxRetryCount, db)
		list, err := clientTarget.ListDatabaseNames(ctx, bson.D{{Key: "name", Value: db}})
		require.NoError(t, err)
		if len(list) == 1 {
			logger.Log.Infof("Database '%s' appeared successfully", db)
			break
		}
		if retryCount == maxRetryCount {
			require.Failf(t, "Didn't wait until database '%s' appear", db)
		}
		time.Sleep(3 * time.Second)
	}

	logger.Log.Infof("Drop database %s", db)
	err = dbSource.Drop(context.Background())
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	logger.Log.Info("Wait until database will perish from destination")

	for retryCount, maxRetryCount := 1, 14; retryCount <= maxRetryCount; retryCount++ {
		logger.Log.Infof("Attempt %d of %d for database '%s' to drop on target", retryCount, maxRetryCount, db)
		list, err := clientTarget.ListDatabaseNames(ctx, bson.D{{Key: "name", Value: db}})
		require.NoError(t, err)
		if len(list) == 0 {
			logger.Log.Infof("Database '%s' dropped successfully", db)
			break
		}
		if retryCount == maxRetryCount {
			require.Failf(t, "Database '%s' should be dropped on target during replication", db)
		}
		time.Sleep(3 * time.Second)
	}
}
