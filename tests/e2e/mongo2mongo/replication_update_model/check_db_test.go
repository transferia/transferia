package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/mongo"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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
	// updates of every kind an update description may hold

	dstClient, err := MakeDstClient(&Target)
	require.NoError(t, err)
	defer func() { _ = dstClient.Close(context.Background()) }()
	dstColl := dstClient.Database("db").Collection("timmyb32r_test")

	ctx := context.Background()
	byID := func(id string) bson.D { return bson.D{{Key: "_id", Value: id}} }
	update := func(id string, update interface{}) {
		result, err := coll.UpdateOne(ctx, byID(id), update)
		require.NoError(t, err)
		require.EqualValues(t, 1, result.MatchedCount)
	}
	set := func(field string, value interface{}) bson.D {
		return bson.D{{Key: "$set", Value: bson.D{{Key: field, Value: value}}}}
	}
	waitSame := func(id string) {
		var src, dst bson.M
		same := assert.Eventually(t, func() bool {
			src, dst = nil, nil
			if err := coll.FindOne(ctx, byID(id)).Decode(&src); err != nil {
				return false
			}
			if err := dstColl.FindOne(ctx, byID(id)).Decode(&dst); err != nil {
				return false
			}
			return assert.ObjectsAreEqual(src, dst)
		}, 60*time.Second, 200*time.Millisecond)
		require.True(t, same, "document %v: source %v, target %v", id, src, dst)
	}

	_, err = coll.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "doc"},
		{Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "y", Value: 2}}},
		{Key: "arr", Value: bson.A{1, 2, 3, 4, 5}},
		{Key: "objs", Value: bson.A{bson.D{{Key: "x", Value: 1}}, bson.D{{Key: "x", Value: 2}}, bson.D{{Key: "x", Value: 3}}}},
	})
	require.NoError(t, err)
	update("doc", set("n.x", 10))
	update("doc", bson.D{{Key: "$unset", Value: bson.D{{Key: "n.y", Value: ""}}}})
	update("doc", bson.D{{Key: "$push", Value: bson.D{{Key: "arr", Value: 6}}}})
	update("doc", set("arr.0", 100))
	// truncates the array (MongoDB 5.0+ reports it as truncatedArrays, older ones as the whole new array)
	update("doc", mongo.Pipeline{set("arr", bson.D{{Key: "$slice", Value: bson.A{"$arr", 3}}})})
	// truncates the array and changes the elements left
	update("doc", mongo.Pipeline{set("objs", bson.A{bson.D{{Key: "x", Value: 10}}, bson.D{{Key: "x", Value: 20}}})})
	update("doc", bson.D{{Key: "$pop", Value: bson.D{{Key: "arr", Value: 1}}}})
	waitSame("doc")

	// the target lost the document: the update must bring it back whole, not as a stub of the changed fields
	_, err = dstColl.DeleteOne(ctx, byID("doc"))
	require.NoError(t, err)
	update("doc", set("n.x", 11))
	waitSame("doc")

	// the document of the target is not the one the update was made for
	_, err = dstColl.UpdateOne(ctx, byID("doc"), set("n", "scalar"))
	require.NoError(t, err)
	update("doc", set("n.x", 12))
	waitSame("doc")

	_, err = coll.ReplaceOne(ctx, byID("doc"), bson.D{{Key: "replaced", Value: true}})
	require.NoError(t, err)
	waitSame("doc")

	// a field with a dot in its name: an update operator would take the name for a path of nested fields
	version, err := provider_mongo.GetVersion(ctx, client, "")
	require.NoError(t, err)
	if version.GTE(provider_mongo.MongoVersion6_1) {
		setDotted := func(value int) mongo.Pipeline {
			return mongo.Pipeline{{{Key: "$replaceWith", Value: bson.D{{Key: "$setField", Value: bson.D{
				{Key: "field", Value: "dot.ted"}, {Key: "input", Value: "$$ROOT"}, {Key: "value", Value: value},
			}}}}}}
		}
		update("doc", setDotted(1))
		update("doc", setDotted(2))
		waitSame("doc")
		var dst bson.M
		require.NoError(t, dstColl.FindOne(ctx, byID("doc")).Decode(&dst))
		require.EqualValues(t, 2, dst["dot.ted"])
	}

	//------------------------------------------------------------------------------------
	// check results

	require.NoError(t, storage.WaitEqualRowsCount(t, "db", "timmyb32r_test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
