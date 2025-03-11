package snapshot

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
	"github.com/transferria/transferria/tests/helpers"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	GoodDatabase = "lawful_good_db"
	BadDatabase  = "yolo234_database"
	Collection   = "some_collection817"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = mongodataagent.MongoSource{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:     os.Getenv("MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Collections: []mongodataagent.MongoCollection{
			{DatabaseName: GoodDatabase, CollectionName: "*"},
			{DatabaseName: BadDatabase, CollectionName: "*"},
		},
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

type DummyData struct {
	Value int
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

func clearSrc(t *testing.T, client *mongodataagent.MongoClientWrapper) {
	t.Helper()
	for _, dbName := range []string{GoodDatabase, BadDatabase} {
		db := client.Database(dbName)
		_ = db.Drop(context.Background())
	}
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
		t.Run("CheckDBAdditionOnSnapshot", CheckDBAdditionOnSnapshot)
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

func CheckDBAdditionOnSnapshot(t *testing.T) {
	client, err := mongodataagent.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	clearSrc(t, client)
	dbOk := client.Database(GoodDatabase)
	collOk := dbOk.Collection(Collection)
	dbNotOk := client.Database(BadDatabase)
	collNotOk := dbNotOk.Collection(Collection)

	logger.Log.Info("prefill both collection with entities in snapshot")
	collectionCount := 20000
	var dummyDataSlice []interface{}
	for i := 0; i < collectionCount; i++ {
		dummyDataSlice = append(dummyDataSlice, DummyData{Value: i})
	}
	var im *mongo.InsertManyResult
	im, err = collOk.InsertMany(context.Background(), dummyDataSlice)
	require.NoError(t, err)
	require.Len(t, im.InsertedIDs, collectionCount)
	im, err = collNotOk.InsertMany(context.Background(), dummyDataSlice)
	require.NoError(t, err)
	require.Len(t, im.InsertedIDs, collectionCount)

	logger.Log.Info("start replication")
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	errChan := make(chan error, 1)
	go func() {
		errChan <- localWorker.Run()
	}()
	defer func() { _ = localWorker.Stop() }()

	logger.Log.Info("just after worker started, let's drop second database and recreate it during snapshot")
	_ = dbNotOk.Drop(context.Background())
	for i := 0; i < 20; i++ {
		_, err := collNotOk.InsertOne(context.Background(), DummyData{Value: i})
		require.NoError(t, err)
	}

	logger.Log.Info("wait for replication fatal error with exact message")
	timer := time.NewTimer(30 * time.Second)
	select {
	case err := <-errChan:
		require.True(t, abstract.IsFatal(err), "should be fatal")
		expectMessage := fmt.Sprintf("Cannot get cluster time for database '%s', try to Activate transfer again. ", BadDatabase)
		require.Contains(t, err.Error(), expectMessage, "Error should be about cluster time for new collection")
		require.Contains(t, err.Error(), BadDatabase, "Should contain bad database name")
	case <-timer.C:
		t.Fatal("Couldn't wait for error from worker")
	}
}
