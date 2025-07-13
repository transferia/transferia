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
	filterrowsbyids "github.com/transferia/transferia/pkg/transformer/registry/filter_rows_by_ids"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	CollectionName = "collection"
	TargetDbName   = "target"
	SourceDbName   = "source"
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

func runTransfer(t *testing.T, source *mongodataagent.MongoSource, target *mongodataagent.MongoDestination) *local.LocalWorker {
	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)

	transformer, err := filterrowsbyids.NewFilterRowsByIDsTransformer(
		filterrowsbyids.Config{
			Tables: filter.Tables{
				IncludeTables: []string{"source.collection"},
			},
			Columns: filter.Columns{
				IncludeColumns: []string{"_id"},
			},
			AllowedIDs: []string{"ID1", "ID2", "ID4"},
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
	t.Run("FilterRowsByIDs", FilterRowsByIDs)
}

func FilterRowsByIDs(t *testing.T) {
	var (
		Source = *mongodataagent.RecipeSource(
			mongodataagent.WithCollections(
				mongodataagent.MongoCollection{DatabaseName: SourceDbName, CollectionName: CollectionName}))
		Target = *mongodataagent.RecipeTarget(
			mongodataagent.WithDatabase(TargetDbName),
		)
	)

	srcClient, targetClient := initEndpoints(t, &Source, &Target)
	defer func() {
		_ = srcClient.Close(context.Background())
		_ = targetClient.Close(context.Background())
	}()

	sourceDb := srcClient.Database(SourceDbName)
	sourceCollection := sourceDb.Collection(CollectionName)

	defer func() {
		_ = sourceCollection.Drop(context.Background())
	}()

	var err error

	// insert initial data
	{
		err = sourceDb.CreateCollection(context.Background(), CollectionName)
		require.NoError(t, err)

		_, err = sourceCollection.InsertOne(context.Background(), bson.M{"_id": "ID0", "column2": 0})
		require.NoError(t, err)

		_, err = sourceCollection.InsertOne(context.Background(), bson.M{"_id": "ID1", "column2": 1})
		require.NoError(t, err)

		_, err = sourceCollection.InsertOne(context.Background(), bson.M{"_id": "ID2", "column2": 2})
		require.NoError(t, err)

		_, err = sourceCollection.InsertOne(context.Background(), bson.M{"_id": "ID3", "column2": 3})
		require.NoError(t, err)
	}

	worker := runTransfer(t, &Source, &Target)
	defer func(worker *local.LocalWorker) {
		_ = worker.Stop()
	}(worker)

	// update while replicating
	{
		_, err = sourceCollection.UpdateOne(context.Background(), bson.M{"_id": "ID0", "column2": 0}, bson.M{"$set": bson.M{"column2": 1}})
		require.NoError(t, err)

		_, err = sourceCollection.UpdateOne(context.Background(), bson.M{"_id": "ID1", "column2": 1}, bson.M{"$set": bson.M{"column2": 2}})
		require.NoError(t, err)

		_, err = sourceCollection.UpdateOne(context.Background(), bson.M{"_id": "ID2", "column2": 2}, bson.M{"$set": bson.M{"column2": 3}})
		require.NoError(t, err)

		_, err = sourceCollection.UpdateOne(context.Background(), bson.M{"_id": "ID3", "column2": 3}, bson.M{"$set": bson.M{"column2": 4}})
		require.NoError(t, err)

		_, err = sourceCollection.InsertOne(context.Background(), bson.M{"_id": "ID4", "column2": 4})
		require.NoError(t, err)
	}

	// check
	{
		require.NoError(t, helpers.WaitDestinationEqualRowsCount(TargetDbName, CollectionName, helpers.GetSampleableStorageByModel(t, Target), 2*time.Minute, 3))

		targetCollection := targetClient.Database(TargetDbName).Collection(CollectionName)
		defer func() {
			_ = targetCollection.Drop(context.Background())
		}()

		db1rowsCount, err := targetCollection.CountDocuments(context.Background(), bson.M{})
		require.NoError(t, err)
		require.Equal(t, int64(3), db1rowsCount)

		var docResult bson.M
		err = targetCollection.FindOne(context.Background(), bson.M{"_id": "ID1", "column2": 2}).Decode(&docResult)
		require.NoError(t, err)
		err = targetCollection.FindOne(context.Background(), bson.M{"_id": "ID2", "column2": 3}).Decode(&docResult)
		require.NoError(t, err)
		err = targetCollection.FindOne(context.Background(), bson.M{"_id": "ID4", "column2": 4}).Decode(&docResult)
		require.NoError(t, err)
	}
}
