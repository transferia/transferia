package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	cpclient "github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	mongodataagent "github.com/transferria/transferria/pkg/providers/mongo"
	ydbStorage "github.com/transferria/transferria/pkg/providers/ydb"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = mongodataagent.MongoSource{
		Hosts:             []string{"localhost"},
		Port:              helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:              os.Getenv("MONGO_LOCAL_USER"),
		Password:          model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		ReplicationSource: mongodataagent.MongoReplicationSourcePerDatabaseUpdateDocument,
	}
	Target = &ydbStorage.YdbDestination{
		Database: os.Getenv("YDB_DATABASE"),
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Instance: os.Getenv("YDB_ENDPOINT"),
	}
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, &Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

//---------------------------------------------------------------------------------------------------------------------
// utils

func LogMongoSource(s *mongodataagent.MongoSource) {
	fmt.Printf("Source.Hosts: %v\n", s.Hosts)
	fmt.Printf("Source.Port: %v\n", s.Port)
	fmt.Printf("Source.User: %v\n", s.User)
	fmt.Printf("Source.Password: %v\n", s.Password)
}

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: Source.Port},
		))
	}()

	if Target.Token == "" {
		Target.Token = "anyNotEmptyString"
	}
	Target.WithDefaults()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Ping", Ping)
		t.Run("Load", Load)
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
}

type Trainer struct {
	Name string
	Age  int
	City string
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
		ID:   helpers.TransferID,
	}
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"db.test_incl"}}

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
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

	result, err := ydbStorage.NewStorage(Target.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(
		t,
		"db",
		"test_incl",
		helpers.GetSampleableStorageByModel(t, Source),
		result,
		60*time.Second,
	))
	require.NoError(t, err)
}
