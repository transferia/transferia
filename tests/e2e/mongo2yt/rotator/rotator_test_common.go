package rotator

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	mongodataagent "github.com/transferia/transferia/pkg/providers/mongo"
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
	ytstorage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

// constants (begin)
const (
	TimeColumnName = "partition_time"
)

var (
	NoneRotation *model.RotatorConfig = nil
	DayRotation                       = &model.RotatorConfig{
		KeepPartCount:     14,
		PartType:          model.RotatorPartDay,
		PartSize:          1,
		TimeColumn:        TimeColumnName,
		TableNameTemplate: "",
	}
)

// constants (end)

var (
	prefillIteration = 0
)

func PrefilledSourceAndTarget() (mongodataagent.MongoSource, ytcommon.YtDestination) {
	prefillIteration += 1
	return mongodataagent.MongoSource{
			Hosts:             []string{"localhost"},
			Port:              helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
			User:              os.Getenv("MONGO_LOCAL_USER"),
			Password:          model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
			ReplicationSource: mongodataagent.MongoReplicationSourcePerDatabaseUpdateDocument,
		}, ytcommon.YtDestination{
			Path:          fmt.Sprintf("//home/cdc/test/mongo2yt/rotator/prefill%d", prefillIteration),
			Cluster:       os.Getenv("YT_PROXY"),
			CellBundle:    "default",
			PrimaryMedium: "default",
		}
}

var (
	GlobalUID = 0
)

type dataRow struct {
	UID int
}

func makeDataRow() dataRow {
	defer func() {
		GlobalUID++
	}()
	return dataRow{
		UID: GlobalUID,
	}
}

func makeAppendTimeMiddleware(rotationTime time.Time) func(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
	return func(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
		newChangeItems := make([]abstract.ChangeItem, 0)
		errors := make([]abstract.TransformerError, 0)
		for _, item := range items {
			if len(item.TableSchema.Columns()) == 0 {
				newChangeItems = append(newChangeItems, item)
				continue
			}
			schemaCopy := item.TableSchema.Columns()[0]
			schemaCopy.ColumnName = TimeColumnName
			schemaCopy.DataType = "datetime"
			schemaCopy.OriginalType = "datetime"
			item.ColumnNames = append(item.ColumnNames, TimeColumnName)
			item.ColumnValues = append(item.ColumnValues, rotationTime)
			item.TableSchema = abstract.NewTableSchema(append(item.TableSchema.Columns(), schemaCopy))
			newChangeItems = append(newChangeItems, item)
		}
		return abstract.TransformerResult{
			Transformed: newChangeItems,
			Errors:      errors,
		}
	}
}

func includeAllTables(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

func ScenarioCheckActivation(
	t *testing.T,
	source mongodataagent.MongoSource,
	target ytcommon.YtDestination,
	table abstract.TableID,
	rotationTime time.Time,
	expectedTablePath ypath.Path,
) {
	targetModel := ytcommon.NewYtDestinationV1(target)
	transferType := abstract.TransferTypeSnapshotOnly
	helpers.InitSrcDst(helpers.TransferID, &source, targetModel, transferType)
	transfer := model.Transfer{
		Type: transferType,
		Src:  &source,
		Dst:  targetModel,
		ID:   helpers.TransferID,
	}
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{table.Fqtn()}}
	// add transformation in order to control rotation
	err := transfer.AddExtraTransformer(helpers.NewSimpleTransformer(t, makeAppendTimeMiddleware(rotationTime), includeAllTables))
	require.NoError(t, err)

	/// ===
	/// Phase I: preload data to source & activate for transferring data to target
	/// ===

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	nodes, err := recursiveListYTNode(context.Background(), ytEnv.YT, ypath.Path(targetModel.Path()))
	logger.Log.Info("Checking cypress path", log.Any("nodes", nodes), log.Error(err))

	// Step: prepare source
	client, err := mongodataagent.Connect(context.Background(), source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	db := client.Database(table.Namespace)
	defer func() {
		// clear collection in the end (for local debug)
		_ = db.Collection(table.Name).Drop(context.Background())
	}()

	err = db.CreateCollection(context.Background(), table.Name)
	require.NoError(t, err)
	coll := db.Collection(table.Name)

	// Step: insert first data item in collection
	_, err = coll.InsertOne(context.Background(), makeDataRow())
	require.NoError(t, err)

	// Step: activate I time to allocate table in target
	wk1 := helpers.Activate(t, &transfer, func(err error) {
		require.NoError(t, err)
	})
	defer wk1.Close(t)

	// Step: check table existence in target
	nodes, err = recursiveListYTNode(context.Background(), ytEnv.YT, ypath.Path(targetModel.Path()))
	logger.Log.Info("Checking cypress path", log.Any("nodes", nodes), log.Error(err))

	ok1, err := ytEnv.YT.NodeExists(context.Background(), expectedTablePath, new(yt.NodeExistsOptions))
	require.NoError(t, err)
	require.True(t, ok1, "table path '%s' should be generated as snapshot result after first activation, but there is nothing", expectedTablePath)

	count1, err := ytstorage.ExactYTTableRowsCount(ytEnv.YT, expectedTablePath)
	require.NoError(t, err)
	require.True(t, count1 > 0, "table path '%s' should be a table with data as a snapshot result after first activation", expectedTablePath)

	tableContent1, err := tablePrinter(context.Background(), ytEnv.YT, expectedTablePath)
	require.NoError(t, err)
	logger.Log.Info("Table content #1", log.Any("content", tableContent1))

	/// ===
	/// Phase II: reload data in source & activate again to check cleanup policy
	/// ===

	// Step: insert second data item in collection (after collection cleanup)
	_, err = coll.DeleteMany(context.Background(), bson.D{})
	require.NoError(t, err)
	_, err = coll.InsertOne(context.Background(), makeDataRow())
	require.NoError(t, err)

	// Step: activate II time to check cleanup policy
	wk2 := helpers.Activate(t, &transfer, func(err error) {
		require.NoError(t, err)
	})
	defer wk2.Close(t)

	// Step: check table existence in target
	ok2, err := ytEnv.YT.NodeExists(context.Background(), expectedTablePath, new(yt.NodeExistsOptions))
	require.NoError(t, err)
	require.True(t, ok2, "table path '%s' should be generated as snapshot result after second activation, but there is nothing", expectedTablePath)

	count2, err := ytstorage.ExactYTTableRowsCount(ytEnv.YT, expectedTablePath)
	require.NoError(t, err)
	require.True(t, count2 > 0, "table path '%s' should be a table with data as a snapshot result after second activation", expectedTablePath)

	tableContent2, err := tablePrinter(context.Background(), ytEnv.YT, expectedTablePath)
	require.NoError(t, err)
	logger.Log.Info("Table content #2", log.Any("content", tableContent2))

	// check count1 and count2 depending on cleanup policy
	cum := targetModel.CleanupMode()
	switch cum {
	case model.Drop:
		require.Equal(t, uint64(1), count2)
	case model.DisabledCleanup:
		require.Equal(t, uint64(1), count2-count1)
	default:
		require.Fail(t, fmt.Sprintf("invalid type of cleanup of YT destination: %v", cum))
	}
}
