package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = provider_mongo.MongoSource{
		Hosts:             []string{"localhost"},
		Port:              testenv.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:              os.Getenv("MONGO_LOCAL_USER"),
		Password:          model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Collections:       []provider_mongo.MongoCollection{{DatabaseName: "db", CollectionName: "timmyb32r_test"}},
		ReplicationSource: provider_mongo.MongoReplicationSourcePerDatabaseUpdateDocument,
	}
	Target = provider_mongo.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     testenv.GetIntFromEnv("DB0_MONGO_LOCAL_PORT"),
		User:     os.Getenv("DB0_MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("DB0_MONGO_LOCAL_PASSWORD")),
		Cleanup:  model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

//---------------------------------------------------------------------------------------------------------------------
// utils

func LogMongoSource(s *provider_mongo.MongoSource) {
	fmt.Printf("Source.Hosts: %v\n", s.Hosts)
	fmt.Printf("Source.Port: %v\n", s.Port)
	fmt.Printf("Source.User: %v\n", s.User)
	fmt.Printf("Source.Password: %v\n", s.Password)
}

func LogMongoDestination(s *provider_mongo.MongoDestination) {
	fmt.Printf("Target.Hosts: %v\n", s.Hosts)
	fmt.Printf("Target.Port: %v\n", s.Port)
	fmt.Printf("Target.User: %v\n", s.User)
	fmt.Printf("Target.Password: %v\n", s.Password)
}

func MakeDstClient(t *provider_mongo.MongoDestination) (*provider_mongo.MongoClientWrapper, error) {
	return provider_mongo.Connect(context.Background(), t.ConnectionOptions([]string{}), nil)
}

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mongo source", Port: Source.Port},
			network.LabeledPort{Label: "Mongo target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Ping", Ping)
		t.Run("Load", Load)
	})
}

func Ping(t *testing.T) {
	// ping src
	LogMongoSource(&Source)
	client, err := provider_mongo.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
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
	client, err := provider_mongo.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
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
		ID:   transferhelpers.TransferID,
	}

	err = tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewFakeClient(), transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// replicate one record

	_, err = coll.InsertOne(context.Background(), Trainer{"b", 2, "bb"})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.D{{Key: "name", Value: "b"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "bb"}, {Key: "age", Value: 21}}}})
	require.NoError(t, err)

	_, err = coll.InsertOne(context.Background(), Trainer{"c", 2, "aa"})
	require.NoError(t, err)
	_, err = coll.UpdateOne(context.Background(), bson.D{{Key: "name", Value: "c"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "cc"}, {Key: "age", Value: 21}}}})
	require.NoError(t, err)
	_, err = coll.UpdateOne(context.Background(), bson.D{{Key: "name", Value: "cc"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "ccc"}, {Key: "age", Value: 21}}}})
	require.NoError(t, err)

	_, err = coll.UpdateMany(context.Background(), bson.M{"age": bson.M{"$lte": 21}}, bson.D{{Key: "$set", Value: bson.D{{Key: "City", Value: "Gotham"}}}})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// check results

	require.NoError(t, storage.WaitEqualRowsCount(t, "db", "timmyb32r_test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
