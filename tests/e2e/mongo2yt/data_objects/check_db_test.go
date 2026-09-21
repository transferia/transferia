package snapshot

import (
	"context"
	"encoding/json"
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
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = provider_mongo.MongoSource{
		Hosts:             []string{"localhost"},
		Port:              testenv.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:              os.Getenv("MONGO_LOCAL_USER"),
		Password:          model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		ReplicationSource: provider_mongo.MongoReplicationSourcePerDatabaseUpdateDocument,
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/mongo2yt_e2e")
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

//---------------------------------------------------------------------------------------------------------------------
// utils

func LogMongoSource(s *provider_mongo.MongoSource) {
	fmt.Printf("Source.Hosts: %v\n", s.Hosts)
	fmt.Printf("Source.Port: %v\n", s.Port)
	fmt.Printf("Source.User: %v\n", s.User)
	fmt.Printf("Source.Password: %v\n", s.Password)
}

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mongo source", Port: Source.Port},
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
}

type Trainer struct {
	Name string
	Age  int
	City string
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
		_ = db.Collection("test_incl").Drop(context.Background())
		_ = db.Collection("test_excl").Drop(context.Background())
	}()

	err = db.CreateCollection(context.Background(), "test_incl")
	require.NoError(t, err)
	coll := db.Collection("test_incl")
	_, err = coll.InsertOne(context.Background(), Trainer{"a", 1, "aa"})
	require.NoError(t, err)

	err = db.CreateCollection(context.Background(), "test_excl")
	require.NoError(t, err)
	exclCol := db.Collection("test_excl")
	_, err = exclCol.InsertOne(context.Background(), Trainer{"a", 1, "aa"})
	require.NoError(t, err)
	//------------------------------------------------------------------------------------
	// start worker

	transfer := model.Transfer{
		Type: abstract.TransferTypeSnapshotAndIncrement,
		Src:  &Source,
		Dst:  Target,
		ID:   transferhelpers.TransferID,
	}
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"db.test_incl"}}

	err = tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewFakeClient(), transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// replicate one record

	_, err = coll.InsertOne(context.Background(), Trainer{"b", 2, "bb"})
	require.NoError(t, err)

	_, err = exclCol.InsertOne(context.Background(), Trainer{"b", 2, "bb"})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// check results

	require.NoError(t, storage.WaitEqualRowsCount(
		t,
		"db",
		"test_incl",
		storagecomparison.GetSampleableStorageByModel(t, Source),
		storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel()),
		60*time.Second,
	))

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	exists, err := ytEnv.YT.NodeExists(context.Background(), ypath.Path(Target.Path()).Child("db_test_excl"), nil)
	require.NoError(t, err)
	require.False(t, exists)

	//------------------------------------------------------------------------------------
	// extract and check that numbers transferred as strings (DTSUPPORT-6418)

	result := make([]abstract.ChangeItem, 0)
	storage, err := yt_storage.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
	err = storage.LoadTable(context.Background(), abstract.TableDescription{Name: "db_test_incl"}, func(input []abstract.ChangeItem) error {
		result = append(result, input...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(result))

	for _, item := range result {
		q, _ := json.Marshal(item)
		fmt.Println("QQQ", string(q))

		document := item.AsMap()["document"]
		documentUnp := document.(map[string]interface{})
		age := documentUnp["age"]
		require.True(t, age == "1" || age == "2")
	}
}
