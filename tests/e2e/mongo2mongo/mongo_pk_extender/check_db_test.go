package snapshot

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	mongodataagent "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/pkg/transformer/registry/mongo_pk_extender"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = *mongodataagent.RecipeSource(
		mongodataagent.WithCollections(
			mongodataagent.MongoCollection{DatabaseName: "db", CollectionName: "timmyb32r_test"}))
	Target = *mongodataagent.RecipeTarget(mongodataagent.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

//---------------------------------------------------------------------------------------------------------------------
// utils

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
		t.Run("ExtendSimplePK", ExtendSimplePK)
		t.Run("ExtendCompositePK", ExtendCompositePK)
		t.Run("CollapseToCompositePK", CollapseToCompositePK)
		t.Run("CollapseToSimplePK", CollapseToSimplePK)
	})
}

func ExtendSimplePK(t *testing.T) {
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

	type Actor struct {
		Name string
		Age  int
		City string
	}

	_, err = coll.InsertOne(context.Background(), Actor{Name: "Daniel Craig", Age: 47, City: "Chester, England"})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "Daniel Craig"}, bson.M{"$set": bson.M{"age": 57}})
	require.NoError(t, err)

	_, err = coll.InsertOne(context.Background(), Actor{Name: "Ryan Gosling", Age: 44, City: "London, Ontario"})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	transformer, err := mongo_pk_extender.NewMongoPKExtenderTransformer(
		mongo_pk_extender.Config{
			Expand:          true,
			ExtraFieldName:  "orgId",
			ExtraFieldValue: "42",
			Tables: filter.Tables{
				ExcludeTables: []string{"__dt_cluster_time"},
			},
		},
		logger.Log,
	)
	require.NoError(t, err)
	helpers.AddTransformer(t, transfer, transformer)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// check results

	_, err = coll.InsertOne(context.Background(), Actor{Name: "Tom Holland", Age: 38, City: "London, England"})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "Tom Holland"}, bson.M{"$set": bson.M{"age": 28}})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "Ryan Gosling"}, bson.M{"$set": bson.M{"age": 66}})
	require.NoError(t, err)

	_, err = coll.DeleteOne(context.Background(), bson.M{"name": "Daniel Craig"})
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "db", "timmyb32r_test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))

	targetClient, err := MakeDstClient(&Target)
	require.NoError(t, err)

	defer func() { _ = targetClient.Close(context.Background()) }()

	targetColl := targetClient.Database("db").Collection("timmyb32r_test")

	deletedDocumentCount, err := targetColl.CountDocuments(context.Background(), bson.M{"name": "Daniel Craig"})
	require.NoError(t, err)
	require.Equal(t, int64(1), deletedDocumentCount)

	compositePKdocumentsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.orgId": 42})
	require.NoError(t, err)
	require.Equal(t, int64(2), compositePKdocumentsCount)
}

func ExtendCompositePK(t *testing.T) {
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

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "car"}}}, {Key: "name", Value: "Opel"}, {Key: "power", Value: 100}})
	require.NoError(t, err)

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "bike"}}}, {Key: "name", Value: "Yamaha"}, {Key: "power", Value: 250}})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "Opel"}, bson.M{"$set": bson.M{"power": 105}})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	transformer, err := mongo_pk_extender.NewMongoPKExtenderTransformer(
		mongo_pk_extender.Config{
			Expand:          true,
			ExtraFieldName:  "orgId",
			ExtraFieldValue: "42",
			Tables: filter.Tables{
				ExcludeTables: []string{"__dt_cluster_time"},
			},
		},
		logger.Log,
	)
	require.NoError(t, err)
	helpers.AddTransformer(t, transfer, transformer)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// check results

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "id", Value: 2}, {Key: "category", Value: "car"}}}, {Key: "name", Value: "Porsche"}, {Key: "power", Value: 200}})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "Yamaha"}, bson.M{"$set": bson.M{"power": 333}})
	require.NoError(t, err)

	_, err = coll.DeleteOne(context.Background(), bson.M{"name": "Opel"})
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "db", "timmyb32r_test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))

	targetClient, err := MakeDstClient(&Target)
	require.NoError(t, err)

	defer func() { _ = targetClient.Close(context.Background()) }()

	targetColl := targetClient.Database("db").Collection("timmyb32r_test")

	deletedDocumentCount, err := targetColl.CountDocuments(context.Background(), bson.M{"name": "Opel"})
	require.NoError(t, err)
	require.Equal(t, int64(1), deletedDocumentCount)

	compositePKdocumentsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.orgId": 42})
	require.NoError(t, err)
	require.Equal(t, int64(2), compositePKdocumentsCount)

	carsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.id.category": "car"})
	require.NoError(t, err)
	require.Equal(t, int64(1), carsCount)
}

func CollapseToCompositePK(t *testing.T) {
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

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 42}, {Key: "id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "car"}}}}}, {Key: "name", Value: "Porsche"}, {Key: "power", Value: 200}})
	require.NoError(t, err)

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 42}, {Key: "id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "bike"}}}}}, {Key: "name", Value: "Yamaha"}, {Key: "power", Value: 250}})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "Porsche"}, bson.M{"$set": bson.M{"power": 300}})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	transformer, err := mongo_pk_extender.NewMongoPKExtenderTransformer(
		mongo_pk_extender.Config{
			Expand:          false,
			ExtraFieldName:  "orgId",
			ExtraFieldValue: "42",
			Tables: filter.Tables{
				ExcludeTables: []string{"__dt_cluster_time"},
			},
		},
		logger.Log,
	)
	require.NoError(t, err)
	helpers.AddTransformer(t, transfer, transformer)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// check results

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 42}, {Key: "id", Value: bson.D{{Key: "id", Value: 2}, {Key: "category", Value: "car"}}}}}, {Key: "name", Value: "BMW"}, {Key: "power", Value: 400}})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "BMW"}, bson.M{"$set": bson.M{"power": 350}})
	require.NoError(t, err)

	_, err = coll.DeleteOne(context.Background(), bson.M{"name": "Porsche"})
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "db", "timmyb32r_test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))

	targetClient, err := MakeDstClient(&Target)
	require.NoError(t, err)

	defer func() { _ = targetClient.Close(context.Background()) }()

	targetColl := targetClient.Database("db").Collection("timmyb32r_test")

	deletedDocumentCount, err := targetColl.CountDocuments(context.Background(), bson.M{"name": "Porsche"})
	require.NoError(t, err)
	require.Equal(t, int64(1), deletedDocumentCount)

	compositePKdocumentsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.orgId": 42})
	require.NoError(t, err)
	require.Equal(t, int64(0), compositePKdocumentsCount)
}

func CollapseToSimplePK(t *testing.T) {
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

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 42}, {Key: "id", Value: 1}}}, {Key: "name", Value: "first"}, {Key: "count", Value: 10}})
	require.NoError(t, err)

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 42}, {Key: "id", Value: 2}}}, {Key: "name", Value: "second"}, {Key: "count", Value: 25}})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "first"}, bson.M{"$set": bson.M{"count": 12}})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	transformer, err := mongo_pk_extender.NewMongoPKExtenderTransformer(
		mongo_pk_extender.Config{
			Expand:          false,
			ExtraFieldName:  "orgId",
			ExtraFieldValue: "42",
			Tables: filter.Tables{
				ExcludeTables: []string{"__dt_cluster_time"},
			},
		},
		logger.Log,
	)
	require.NoError(t, err)
	helpers.AddTransformer(t, transfer, transformer)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// check results

	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 42}, {Key: "id", Value: 3}}}, {Key: "name", Value: "third"}, {Key: "count", Value: 55}})
	require.NoError(t, err)

	_, err = coll.UpdateOne(context.Background(), bson.M{"name": "third"}, bson.M{"$set": bson.M{"count": 66}})
	require.NoError(t, err)

	_, err = coll.DeleteOne(context.Background(), bson.M{"name": "first"})
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "db", "timmyb32r_test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))

	targetClient, err := MakeDstClient(&Target)
	require.NoError(t, err)

	defer func() { _ = targetClient.Close(context.Background()) }()

	targetColl := targetClient.Database("db").Collection("timmyb32r_test")

	deletedDocumentCount, err := targetColl.CountDocuments(context.Background(), bson.M{"name": "first"})
	require.NoError(t, err)
	require.Equal(t, int64(1), deletedDocumentCount)

	compositePKdocumentsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.orgId": 42})
	require.NoError(t, err)
	require.Equal(t, int64(0), compositePKdocumentsCount)
}
