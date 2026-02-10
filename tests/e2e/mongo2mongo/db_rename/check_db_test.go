package snapshot

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	mongocommon "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	ctx    = context.Background()
	Source = mongocommon.MongoSource{
		Hosts:       []string{"localhost"},
		Port:        helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:        os.Getenv("MONGO_LOCAL_USER"),
		Password:    model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Collections: []mongocommon.MongoCollection{},
	}
	Target = mongocommon.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("DB0_MONGO_LOCAL_PORT"),
		Database: "custom_target_db",
		User:     os.Getenv("DB0_MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("DB0_MONGO_LOCAL_PASSWORD")),
	}
)

//---------------------------------------------------------------------------------------------------------------------
// Utils

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
	return mongocommon.Connect(ctx, t.ConnectionOptions([]string{}), nil)
}

//---------------------------------------------------------------------------------------------------------------------
// Source db name NOT given and target db name given

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
	// Ping src
	LogMongoSource(&Source)
	client, err := mongocommon.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	err = client.Ping(ctx, nil)
	require.NoError(t, err)

	// Ping dst
	LogMongoDestination(&Target)
	client2, err := MakeDstClient(&Target)
	require.NoError(t, err)
	err = client2.Ping(ctx, nil)
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	client, err := mongocommon.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// Insert one record into each db in the source
	// They must later show up in the target (in a single custom-named db)

	originalDB1 := client.Database("original_db_1")
	err = originalDB1.CreateCollection(ctx, "grass_pokemon")
	require.NoError(t, err)
	originalDB2 := client.Database("original_db_2")
	err = originalDB2.CreateCollection(ctx, "fire_pokemon")
	require.NoError(t, err)

	grassPokemon := originalDB1.Collection("grass_pokemon")
	firePokemon := originalDB2.Collection("fire_pokemon")

	bulbasaur := bson.D{{
		Key:   "Name",
		Value: "Bulbasaur",
	}}
	charmander := bson.D{{
		Key:   "Name",
		Value: "Charmander",
	}}

	_, err = grassPokemon.InsertOne(ctx, bulbasaur)
	require.NoError(t, err)
	_, err = firePokemon.InsertOne(ctx, charmander)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// Upload snapshot

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)
	_ = helpers.Activate(t, transfer)

	//------------------------------------------------------------------------------------
	// Check results

	targetClient, err := mongocommon.Connect(ctx, Target.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	// Both original dbs must be absent in target (contents must appear in renamed db)

	originalDB1 = targetClient.Database("original_db_1")
	res, err := originalDB1.ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	require.Len(t, res, 0)

	originalDB2 = targetClient.Database("original_db_2")
	res, err = originalDB2.ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	require.Len(t, res, 0)

	renamedDB := targetClient.Database("custom_target_db")
	res, err = renamedDB.ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	resStr := strings.Join(res, ", ")
	// Both collections (from the 2 original dbs) must appear here
	require.Len(t, res, 2, "Collections: %s", resStr)
	require.Contains(t, resStr, "grass_pokemon")
	require.Contains(t, resStr, "fire_pokemon")

	grassColl := renamedDB.Collection("grass_pokemon")
	fireColl := renamedDB.Collection("fire_pokemon")

	var docResult bson.M
	err = grassColl.FindOne(ctx, bulbasaur).Decode(&docResult)
	require.NoError(t, err)
	err = fireColl.FindOne(ctx, charmander).Decode(&docResult)
	require.NoError(t, err)
}
