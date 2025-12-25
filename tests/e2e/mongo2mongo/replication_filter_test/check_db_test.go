package replication_filter_test

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors/codes"
	mongodataagent "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

// creates source from environment settings/recipe
func sourceFromConfig() (*mongodataagent.MongoSource, error) {
	srcPort, err := strconv.Atoi(os.Getenv("MONGO_LOCAL_PORT"))
	if err != nil {
		return nil, err
	}
	ret := new(mongodataagent.MongoSource)
	ret.Hosts = []string{"localhost"}
	ret.Port = srcPort
	ret.User = os.Getenv("MONGO_LOCAL_USER")
	ret.Password = model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD"))
	ret.WithDefaults()
	return ret, nil
}

func targetFromConfig() (*mongodataagent.MongoDestination, error) {
	trgPort, err := strconv.Atoi(os.Getenv("DB0_MONGO_LOCAL_PORT"))
	if err != nil {
		return nil, err
	}
	ret := new(mongodataagent.MongoDestination)
	ret.Hosts = []string{"localhost"}
	ret.Port = trgPort
	ret.User = os.Getenv("DB0_MONGO_LOCAL_USER")
	ret.Password = model.SecretString(os.Getenv("DB0_MONGO_LOCAL_PASSWORD"))
	ret.Cleanup = model.Drop
	return ret, nil
}

func makeTransfer(id string, source *mongodataagent.MongoSource, target *mongodataagent.MongoDestination) *model.Transfer {
	source.SlotID = id // set slot ID in order to get valid cluster time on ActivateDelivery

	transfer := new(model.Transfer)
	transfer.Type = abstract.TransferTypeSnapshotAndIncrement
	transfer.Src = source
	transfer.Dst = target
	transfer.ID = id
	transfer.WithDefault()
	transfer.FillDependentFields()
	return transfer
}

func TestGroup(t *testing.T) {
	sourcePort, err := strconv.Atoi(os.Getenv("MONGO_LOCAL_PORT"))
	require.NoError(t, err)
	targetPort, err := strconv.Atoi(os.Getenv("DB0_MONGO_LOCAL_PORT"))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: sourcePort},
			helpers.LabeledPort{Label: "Mongo target", Port: targetPort},
		))
	}()

	t.Run("Empty Collection List Means Include All", testEmptyCollectionListIncludesAll)
	t.Run("Include All Collections Test", testCollectionFilterIncludeWholeDB)
	t.Run("Empty Set Collections Test", testCollectionFilterAllIncludesExcluded)
	t.Run("Exclude Star Wins Include Star", testCollectionFilterWholeDBExcludedExcludesCollection)
}

func testCollectionFilterIncludeWholeDB(t *testing.T) {
	t.Parallel()

	src, err := sourceFromConfig()
	require.NoError(t, err)
	tgt, err := targetFromConfig()
	require.NoError(t, err)

	src.Collections = []mongodataagent.MongoCollection{
		{DatabaseName: "db1", CollectionName: "*"},
	}

	transfer := makeTransfer("transfer1", src, tgt)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	if strings.Contains(err.Error(), "replication") {
		require.EqualError(t, err, "Failed in accordance with configuration: Some tables whose replication was requested are missing in the source database. Include directives with no matching tables: [db1.*]")
	} else {
		require.True(t, codes.NoTablesFound.Contains(err))
	}
}

func testCollectionFilterAllIncludesExcluded(t *testing.T) {
	t.Parallel()

	src, err := sourceFromConfig()
	require.NoError(t, err)
	tgt, err := targetFromConfig()
	require.NoError(t, err)

	src.Collections = []mongodataagent.MongoCollection{
		{DatabaseName: "db1", CollectionName: "coll1"},
		{DatabaseName: "db1", CollectionName: "coll2"},
		{DatabaseName: "db2", CollectionName: "A"},
		{DatabaseName: "db2", CollectionName: "B"},
	}
	// exclude elides all included collections
	src.ExcludedCollections = []mongodataagent.MongoCollection{
		{DatabaseName: "db2", CollectionName: "B"},
		{DatabaseName: "db2", CollectionName: "C"},
		{DatabaseName: "db1", CollectionName: "coll3"},
		{DatabaseName: "db1", CollectionName: "coll1"},
		{DatabaseName: "db2", CollectionName: "A"},
		{DatabaseName: "db1", CollectionName: "coll2"},
	}

	logger.Log.Info("start replication")
	transfer := makeTransfer("transfer2", src, tgt)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.Error(t, err)
	if strings.Contains(err.Error(), "replication") {
		require.Contains(t, err.Error(), "Failed in accordance with configuration: Some tables whose replication was requested are missing in the source database. Include directives with no matching tables:")
		require.Contains(t, err.Error(), "db1.coll1")
		require.Contains(t, err.Error(), "db1.coll2")
		require.Contains(t, err.Error(), "db2.A")
		require.Contains(t, err.Error(), "db2.B")
	} else {
		require.Contains(t, err.Error(), "Unable to find any tables")
	}
}

func testEmptyCollectionListIncludesAll(t *testing.T) {
	logger.Log.Warn("Waring -- this test can NOT be run in parallel")

	src, err := sourceFromConfig()
	require.NoError(t, err)
	tgt, err := targetFromConfig()
	require.NoError(t, err)

	srcClient, err := mongodataagent.Connect(context.Background(), src.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	ldb, err := srcClient.ListDatabases(context.Background(), bson.D{})
	require.NoError(t, err)
	for _, db := range ldb.Databases {
		_ = srcClient.Database(db.Name).Drop(context.Background())
	}

	db1, db2 := "A", "B"
	coll1, coll2 := "C", "D"
	type PingData struct{ version int }

	insertRandomDocuments := func() {
		t.Helper()
		for _, db := range []string{db1, db2} {
			for _, coll := range []string{coll1, coll2} {
				_, err = srcClient.Database(db).Collection(coll).InsertOne(context.Background(), PingData{version: rand.Int()})
				if err != nil {
					require.NoError(t, err, "Couldn't insert into database one item. Producing goroutine stops.")
					return
				}
			}
		}
	}

	logger.Log.Info("Create databases to save cluster time")
	insertRandomDocuments()

	logger.Log.Info("Create and activate transfer")
	transfer := makeTransfer("transfer3", src, tgt)
	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	logger.Log.Info("Insert documents after activation")
	insertRandomDocuments()

	logger.Log.Info("Start replication worker")
	replicationWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, solomon.NewRegistry(nil), logger.Log)
	errChan := make(chan error, 1)
	var wgWaitForStart sync.WaitGroup
	wgWaitForStart.Add(1)
	go func() {
		wgWaitForStart.Done()
		errChan <- replicationWorker.Run()
	}()
	logger.Log.Info("wait for goroutine to start")
	wgWaitForStart.Wait()

	logger.Log.Info("wait for appropriate error from replication")
	timeToWait := 10 * time.Second
	timer := time.NewTimer(timeToWait)
	select {
	case err := <-errChan:
		require.NoError(t, err, "Should be no error")
	case <-timer.C:
		logger.Log.Infof("OK, replication didn't fail within time interval '%v'", timeToWait)
		break
	}
}

func testCollectionFilterWholeDBExcludedExcludesCollection(t *testing.T) {
	t.Parallel()

	src, err := sourceFromConfig()
	require.NoError(t, err)
	tgt, err := targetFromConfig()
	require.NoError(t, err)

	src.Collections = []mongodataagent.MongoCollection{
		{DatabaseName: "db1", CollectionName: "coll1"},
	}
	// exclude elides previous collections
	src.ExcludedCollections = []mongodataagent.MongoCollection{
		{DatabaseName: "db1", CollectionName: "*"},
	}

	logger.Log.Info("start replication")
	transfer := makeTransfer("transfer4", src, tgt)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	if strings.Contains(err.Error(), "replication") {
		require.EqualError(t, err, "Failed in accordance with configuration: Some tables whose replication was requested are missing in the source database. Include directives with no matching tables: [db1.coll1]")
	} else {
		require.True(t, codes.NoTablesFound.Contains(err))
	}
}
