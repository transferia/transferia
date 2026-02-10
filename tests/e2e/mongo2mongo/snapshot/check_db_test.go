package snapshot

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	mongocommon "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/tests/helpers"
)

const databaseName string = "db"

var (
	Source = *mongocommon.RecipeSource(
		mongocommon.WithCollections(
			mongocommon.MongoCollection{DatabaseName: databaseName, CollectionName: "timmyb32r_test"},
			mongocommon.MongoCollection{DatabaseName: databaseName, CollectionName: "empty"},
		),
	)
	Target = *mongocommon.RecipeTarget(mongocommon.WithPrefix("DB0_"))
)

//---------------------------------------------------------------------------------------------------------------------
// utils

func LogMongoSource(s *mongocommon.MongoSource) {
	fmt.Printf("Source.Hosts: %v\n", s.Hosts)
	fmt.Printf("Source.Port: %v\n", s.Port)
	fmt.Printf("Source.User: %v\n", s.User)
	fmt.Printf("Source.Password: %v\n", s.Password)
}

func LogMongoDestination(s *mongocommon.MongoDestination) {
	fmt.Printf("Target.Hosts: %v\n", s.Hosts)
	fmt.Printf("Target.Port: %v\n", s.Port)
	fmt.Printf("Target.User: %v\n", s.User)
	fmt.Printf("Target.Password: %v\n", s.Password)
}

func MakeDstClient(t *mongocommon.MongoDestination) (*mongocommon.MongoClientWrapper, error) {
	return mongocommon.Connect(context.Background(), t.ConnectionOptions([]string{}), nil)
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
		t.Run("Snapshot", Snapshot)
	})
}

func Ping(t *testing.T) {
	// ping src
	LogMongoSource(&Source)
	client, err := mongocommon.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	err = client.Ping(context.TODO(), nil)
	require.NoError(t, err)

	// ping dst
	LogMongoDestination(&Target)
	client2, err := MakeDstClient(&Target)
	require.NoError(t, err)
	err = client2.Ping(context.TODO(), nil)
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	client, err := mongocommon.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// insert one record

	db := client.Database(databaseName)
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

	err = db.CreateCollection(context.Background(), "empty")
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// upload snapshot

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)

	//------------------------------------------------------------------------------------
	// check results

	err = helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams())
	require.NoError(t, err)
}
