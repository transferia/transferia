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
	CollectionName = "issues"
	CommonDbName   = "common"
	FirstDbName    = "first"
	SecondDbName   = "second"
)

func initEndpoints(t *testing.T, source *mongodataagent.MongoSource, target *mongodataagent.MongoDestination) (*mongodataagent.MongoClientWrapper, *mongodataagent.MongoClientWrapper) {
	_ = os.Setenv("YC", "1")

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: source.Port},
			helpers.LabeledPort{Label: "Mongo target", Port: target.Port},
		))
	}()

	srcClient, err := mongodataagent.Connect(context.Background(), source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	targetClient, err := mongodataagent.Connect(context.Background(), target.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	return srcClient, targetClient
}

func runTransfer(t *testing.T, source *mongodataagent.MongoSource, target *mongodataagent.MongoDestination, expand bool) *local.LocalWorker {
	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)

	transformer, err := mongo_pk_extender.NewMongoPKExtenderTransformer(
		mongo_pk_extender.Config{
			Expand:              expand,
			DiscriminatorField:  "orgId",
			DiscriminatorValues: []mongo_pk_extender.SchemaDiscriminator{{Schema: FirstDbName, Value: "24"}, {Schema: SecondDbName, Value: "81"}},
			Tables: filter.Tables{
				ExcludeTables: []string{mongodataagent.ClusterTimeCollName},
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
	return localWorker
}

func Test_Group(t *testing.T) {
	t.Run("SimpleFromMultipleToCommon", SimpleFromMultipleToCommon)
	t.Run("SimpleFromCommonToMultiple", SimpleFromCommonToMultiple)
	t.Run("CompositeFromMultipleToCommon", CompositeFromMultipleToCommon)
	t.Run("CompositeFromCommonToMultiple", CompositeFromCommonToMultiple)
}

func SimpleFromMultipleToCommon(t *testing.T) {
	var (
		Source = *mongodataagent.RecipeSource(
			mongodataagent.WithCollections(
				mongodataagent.MongoCollection{DatabaseName: FirstDbName, CollectionName: CollectionName},
				mongodataagent.MongoCollection{DatabaseName: SecondDbName, CollectionName: CollectionName}))
		Target = *mongodataagent.RecipeTarget(
			mongodataagent.WithPrefix("DB0_"),
			mongodataagent.WithDatabase(CommonDbName),
		)
	)

	srcClient, targetClient := initEndpoints(t, &Source, &Target)
	defer func() {
		_ = srcClient.Close(context.Background())
		_ = targetClient.Close(context.Background())
	}()

	db1 := srcClient.Database(FirstDbName)
	db1Coll := db1.Collection(CollectionName)

	db2 := srcClient.Database(SecondDbName)
	db2Coll := db2.Collection(CollectionName)

	defer func() {
		_ = db1Coll.Drop(context.Background())
		_ = db2Coll.Drop(context.Background())
	}()

	var err error

	// insert initial data
	{
		err = db1.CreateCollection(context.Background(), CollectionName)
		require.NoError(t, err)

		err = db2.CreateCollection(context.Background(), CollectionName)
		require.NoError(t, err)

		_, err = db1Coll.InsertOne(context.Background(), bson.D{{Key: "queue", Value: "SUPPORT"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = db1Coll.InsertOne(context.Background(), bson.D{{Key: "queue", Value: "SUPPORT"}, {Key: "number", Value: 2}})
		require.NoError(t, err)

		_, err = db2Coll.InsertOne(context.Background(), bson.D{{Key: "queue", Value: "DEVELOP"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = db2Coll.InsertOne(context.Background(), bson.D{{Key: "queue", Value: "DEVELOP"}, {Key: "number", Value: 2}})
		require.NoError(t, err)
	}

	worker := runTransfer(t, &Source, &Target, true)
	defer func(worker *local.LocalWorker) {
		_ = worker.Stop()
	}(worker)

	// update while replicating
	{
		_, err = db1Coll.InsertOne(context.Background(), bson.D{{Key: "queue", Value: "SERVICE"}, {Key: "number", Value: 4}})
		require.NoError(t, err)

		_, err = db1Coll.UpdateOne(context.Background(), bson.M{"queue": "SUPPORT", "number": 1}, bson.M{"$set": bson.M{"queue": "JUNK"}})
		require.NoError(t, err)

		_, err = db2Coll.DeleteOne(context.Background(), bson.M{"queue": "DEVELOP", "number": 1})
		require.NoError(t, err)

		_, err = db2Coll.InsertOne(context.Background(), bson.D{{Key: "queue", Value: "SERVICE"}, {Key: "number", Value: 5}})
		require.NoError(t, err)
	}

	// check
	{
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(CommonDbName, CollectionName, helpers.GetSampleableStorageByModel(t, Target), 2*time.Minute, 5))

		targetColl := targetClient.Database(CommonDbName).Collection(CollectionName)
		defer func() {
			_ = targetColl.Drop(context.Background())
		}()

		db1rowsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.orgId": 24})
		require.NoError(t, err)
		require.Equal(t, int64(3), db1rowsCount)

		db2rowsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.orgId": 81})
		require.NoError(t, err)
		require.Equal(t, int64(2), db2rowsCount)
	}
}

func SimpleFromCommonToMultiple(t *testing.T) {
	var (
		Source = *mongodataagent.RecipeSource(
			mongodataagent.WithCollections(
				mongodataagent.MongoCollection{DatabaseName: CommonDbName, CollectionName: CollectionName}))
		Target = *mongodataagent.RecipeTarget(
			mongodataagent.WithPrefix("DB0_"),
		)
	)

	srcClient, targetClient := initEndpoints(t, &Source, &Target)
	defer func() {
		_ = srcClient.Close(context.Background())
		_ = targetClient.Close(context.Background())
	}()

	srcDb := srcClient.Database(CommonDbName)
	srcColl := srcDb.Collection(CollectionName)
	defer func() {
		_ = srcColl.Drop(context.Background())
	}()

	var err error

	// insert initial data
	{
		err = srcDb.CreateCollection(context.Background(), CollectionName)
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 24}, {Key: "id", Value: 1}}}, {Key: "queue", Value: "SUPPORT"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 24}, {Key: "id", Value: 2}}}, {Key: "queue", Value: "SUPPORT"}, {Key: "number", Value: 2}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 81}, {Key: "id", Value: 1}}}, {Key: "queue", Value: "DEVELOP"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 81}, {Key: "id", Value: 2}}}, {Key: "queue", Value: "DEVELOP"}, {Key: "number", Value: 2}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 666}, {Key: "id", Value: 1}}}, {Key: "queue", Value: "INVALID"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 666}, {Key: "id", Value: 2}}}, {Key: "queue", Value: "INVALID"}, {Key: "number", Value: 2}})
		require.NoError(t, err)
	}

	worker := runTransfer(t, &Source, &Target, false)
	defer func(worker *local.LocalWorker) {
		_ = worker.Stop()
	}(worker)

	// update while replicating
	{
		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 24}, {Key: "id", Value: 3}}}, {Key: "queue", Value: "SERVICE"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = srcColl.UpdateOne(context.Background(), bson.M{"queue": "SUPPORT", "number": 1}, bson.M{"$set": bson.M{"queue": "JUNK"}})
		require.NoError(t, err)

		_, err = srcColl.DeleteOne(context.Background(), bson.M{"queue": "DEVELOP", "number": 1})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 81}, {Key: "id", Value: 3}}}, {Key: "queue", Value: "SERVICE"}, {Key: "number", Value: 1}})
		require.NoError(t, err)
	}

	// check
	{
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(FirstDbName, CollectionName, helpers.GetSampleableStorageByModel(t, Target), time.Minute, 3))
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(SecondDbName, CollectionName, helpers.GetSampleableStorageByModel(t, Target), time.Minute, 2))

		db1SimpleColl := targetClient.Database(FirstDbName).Collection(CollectionName)
		db2SimpleColl := targetClient.Database(SecondDbName).Collection(CollectionName)
		defer func() {
			_ = db1SimpleColl.Drop(context.Background())
			_ = db2SimpleColl.Drop(context.Background())
		}()

		db1rowsCount, err := db1SimpleColl.CountDocuments(context.Background(), bson.M{"_id.orgId": bson.M{"$exists": true}})
		require.NoError(t, err)
		require.Equal(t, int64(0), db1rowsCount)

		db2rowsCount, err := db2SimpleColl.CountDocuments(context.Background(), bson.M{"_id.orgId": bson.M{"$exists": true}})
		require.NoError(t, err)
		require.Equal(t, int64(0), db2rowsCount)
	}
}

func CompositeFromMultipleToCommon(t *testing.T) {
	var (
		Source = *mongodataagent.RecipeSource(
			mongodataagent.WithCollections(
				mongodataagent.MongoCollection{DatabaseName: FirstDbName, CollectionName: CollectionName},
				mongodataagent.MongoCollection{DatabaseName: SecondDbName, CollectionName: CollectionName}))
		Target = *mongodataagent.RecipeTarget(
			mongodataagent.WithPrefix("DB0_"),
			mongodataagent.WithDatabase(CommonDbName),
		)
	)

	srcClient, targetClient := initEndpoints(t, &Source, &Target)
	defer func() {
		_ = srcClient.Close(context.Background())
		_ = targetClient.Close(context.Background())
	}()

	db1 := srcClient.Database(FirstDbName)
	db1Coll := db1.Collection(CollectionName)

	db2 := srcClient.Database(SecondDbName)
	db2Coll := db2.Collection(CollectionName)

	defer func() {
		_ = db1.Collection(CollectionName).Drop(context.Background())
		_ = db2.Collection(CollectionName).Drop(context.Background())
	}()

	var err error

	// insert initial data
	{
		err = db1.CreateCollection(context.Background(), CollectionName)
		require.NoError(t, err)

		_, err = db1Coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "issueId", Value: 1}}}, {Key: "queue", Value: "SUPPORT"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = db1Coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "issueId", Value: 2}}}, {Key: "queue", Value: "SUPPORT"}, {Key: "number", Value: 2}})
		require.NoError(t, err)

		_, err = db2Coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "issueId", Value: 1}}}, {Key: "queue", Value: "DEVELOP"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = db2Coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "issueId", Value: 2}}}, {Key: "queue", Value: "DEVELOP"}, {Key: "number", Value: 2}})
		require.NoError(t, err)
	}

	worker := runTransfer(t, &Source, &Target, true)
	defer func(worker *local.LocalWorker) {
		_ = worker.Stop()
	}(worker)

	// update while replicating
	{
		_, err = db1Coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "issueId", Value: 3}}}, {Key: "queue", Value: "SERVICE"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = db1Coll.UpdateOne(context.Background(), bson.M{"queue": "SUPPORT", "number": 1}, bson.M{"$set": bson.M{"queue": "JUNK"}})
		require.NoError(t, err)

		_, err = db2Coll.DeleteOne(context.Background(), bson.M{"queue": "DEVELOP", "number": 1})
		require.NoError(t, err)

		_, err = db2Coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "issueId", Value: 3}}}, {Key: "queue", Value: "SERVICE"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

	}

	// check
	{
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(CommonDbName, CollectionName, helpers.GetSampleableStorageByModel(t, Target), time.Minute, 5))

		targetColl := targetClient.Database(CommonDbName).Collection(CollectionName)
		defer func() {
			_ = targetColl.Drop(context.Background())
		}()

		db1rowsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.orgId": 24})
		require.NoError(t, err)
		require.Equal(t, int64(3), db1rowsCount)

		db2rowsCount, err := targetColl.CountDocuments(context.Background(), bson.M{"_id.orgId": 81})
		require.NoError(t, err)
		require.Equal(t, int64(2), db2rowsCount)
	}
}

func CompositeFromCommonToMultiple(t *testing.T) {
	var (
		Source = *mongodataagent.RecipeSource(
			mongodataagent.WithCollections(
				mongodataagent.MongoCollection{DatabaseName: CommonDbName, CollectionName: CollectionName}))
		Target = *mongodataagent.RecipeTarget(
			mongodataagent.WithPrefix("DB0_"),
		)
	)

	srcClient, targetClient := initEndpoints(t, &Source, &Target)
	defer func() {
		_ = srcClient.Close(context.Background())
		_ = targetClient.Close(context.Background())
	}()

	srcDb := srcClient.Database(CommonDbName)
	srcColl := srcDb.Collection(CollectionName)
	defer func() {
		_ = srcColl.Drop(context.Background())
	}()

	var err error

	// insert initial data
	{
		err = srcDb.CreateCollection(context.Background(), CollectionName)
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 24}, {Key: "id", Value: bson.D{{Key: "issueId", Value: 1}}}}}, {Key: "queue", Value: "SUPPORT"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 24}, {Key: "id", Value: bson.D{{Key: "issueId", Value: 2}}}}}, {Key: "queue", Value: "SUPPORT"}, {Key: "number", Value: 2}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 81}, {Key: "id", Value: bson.D{{Key: "issueId", Value: 1}}}}}, {Key: "queue", Value: "DEVELOP"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 81}, {Key: "id", Value: bson.D{{Key: "issueId", Value: 2}}}}}, {Key: "queue", Value: "DEVELOP"}, {Key: "number", Value: 2}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 666}, {Key: "id", Value: bson.D{{Key: "issueId", Value: 1}}}}}, {Key: "queue", Value: "INVALID"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 666}, {Key: "id", Value: bson.D{{Key: "issueId", Value: 2}}}}}, {Key: "queue", Value: "INVALID"}, {Key: "number", Value: 2}})
		require.NoError(t, err)
	}

	worker := runTransfer(t, &Source, &Target, false)
	defer func(worker *local.LocalWorker) {
		_ = worker.Stop()
	}(worker)

	// update while replicating
	{
		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 24}, {Key: "id", Value: bson.D{{Key: "issueId", Value: 3}}}}}, {Key: "queue", Value: "SERVICE"}, {Key: "number", Value: 1}})
		require.NoError(t, err)

		_, err = srcColl.UpdateOne(context.Background(), bson.M{"queue": "SUPPORT", "number": 1}, bson.M{"$set": bson.M{"queue": "JUNK"}})
		require.NoError(t, err)

		_, err = srcColl.DeleteOne(context.Background(), bson.M{"queue": "DEVELOP", "number": 1})
		require.NoError(t, err)

		_, err = srcColl.InsertOne(context.Background(), bson.D{{Key: "_id", Value: bson.D{{Key: "orgId", Value: 81}, {Key: "id", Value: bson.D{{Key: "issueId", Value: 3}}}}}, {Key: "queue", Value: "SERVICE"}, {Key: "number", Value: 1}})
		require.NoError(t, err)
	}

	// check
	{
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(FirstDbName, CollectionName, helpers.GetSampleableStorageByModel(t, Target), time.Minute, 3))
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(SecondDbName, CollectionName, helpers.GetSampleableStorageByModel(t, Target), time.Minute, 2))

		db1SimpleColl := targetClient.Database(FirstDbName).Collection(CollectionName)
		db2SimpleColl := targetClient.Database(SecondDbName).Collection(CollectionName)

		defer func() {
			_ = db1SimpleColl.Drop(context.Background())
			_ = db2SimpleColl.Drop(context.Background())
		}()

		db1rowsCount, err := db1SimpleColl.CountDocuments(context.Background(), bson.M{"_id.orgId": bson.M{"$exists": true}})
		require.NoError(t, err)
		require.Equal(t, int64(0), db1rowsCount)

		db2rowsCount, err := db2SimpleColl.CountDocuments(context.Background(), bson.M{"_id.orgId": bson.M{"$exists": true}})
		require.NoError(t, err)
		require.Equal(t, int64(0), db2rowsCount)
	}
}
