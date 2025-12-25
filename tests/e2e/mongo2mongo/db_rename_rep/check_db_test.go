package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	mongocommon "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	ctx          = context.Background()
	targetDBName = "custom_db_name"
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = mongocommon.MongoSource{
		Hosts:       []string{"localhost"},
		Port:        helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:        os.Getenv("MONGO_LOCAL_USER"),
		Password:    model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Collections: []mongocommon.MongoCollection{},
	}
	Target = mongocommon.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("DB0_MONGO_LOCAL_PORT"),
		Database: targetDBName,
		User:     os.Getenv("DB0_MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("DB0_MONGO_LOCAL_PASSWORD")),
		Cleanup:  model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1") // Do not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

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
// Both source db name and target db name given

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mongo target", Port: Target.Port},
		))
	}()

	t.Run("Replication test", func(t *testing.T) {
		t.Run("Ping", Ping)
		t.Run("Load", Load)
	})
}

func Ping(t *testing.T) {
	// Ping src
	LogMongoSource(&Source)
	client, err := mongocommon.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	defer func() { _ = client.Close(ctx) }()
	require.NoError(t, err)
	err = client.Ping(ctx, nil)
	require.NoError(t, err)

	// Ping dst
	LogMongoDestination(&Target)
	client2, err := MakeDstClient(&Target)
	defer func() { _ = client2.Close(ctx) }()
	require.NoError(t, err)
	err = client2.Ping(ctx, nil)
	require.NoError(t, err)
}

func Load(t *testing.T) {
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

	srcGrassColl := originalDB1.Collection("grass_pokemon")
	srcFireColl := originalDB2.Collection("fire_pokemon")

	bulbasaur := bson.D{{
		Key:   "Name",
		Value: "Bulbasaur",
	}}
	charmander := bson.D{{
		Key:   "Name",
		Value: "Charmander",
	}}

	_, err = srcGrassColl.InsertOne(ctx, bulbasaur)
	require.NoError(t, err)
	_, err = srcFireColl.InsertOne(ctx, charmander)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// Start worker

	transfer := model.Transfer{
		Type: abstract.TransferTypeSnapshotAndIncrement,
		Src:  &Source,
		Dst:  &Target,
		ID:   helpers.TransferID,
	}

	err = tasks.ActivateDelivery(ctx, nil, cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// Add data to existing collections for replication

	ivysaur := bson.D{{
		Key:   "Name",
		Value: "Ivysaur",
	}}
	charmeleon := bson.D{{
		Key:   "Name",
		Value: "Charmeleon",
	}}

	_, err = srcGrassColl.InsertOne(ctx, ivysaur)
	require.NoError(t, err)
	_, err = srcFireColl.InsertOne(ctx, charmeleon)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// Wait for documents to appear in source

	docCount := map[string]int64{}
	expectedCtGrass := int64(2)
	expectedCtFire := int64(2)
	for ct, lim := 1, 14; ct <= lim; ct++ {
		docCount["grass_pokemon"], err = srcGrassColl.CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		docCount["fire_pokemon"], err = srcFireColl.CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		if docCount["grass_pokemon"] == expectedCtGrass && docCount["fire_pokemon"] == expectedCtFire {
			break
		}
		time.Sleep(3 * time.Second)
	}

	require.Equal(t, expectedCtGrass, docCount["grass_pokemon"], "Wrong doc count in grass_pokemon in source")
	require.Equal(t, expectedCtFire, docCount["fire_pokemon"], "Wrong doc count in fire_pokemon in source")

	//------------------------------------------------------------------------------------
	// Check results

	targetClient, err := mongocommon.Connect(ctx, Target.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	renamedDB := targetClient.Database(targetDBName)

	trgGrassColl := renamedDB.Collection("grass_pokemon")
	trgFireColl := renamedDB.Collection("fire_pokemon")

	docCount = map[string]int64{}
	// Wait for documents to appear in target
	for ct, lim := 1, 14; ct <= lim; ct++ {
		docCount["grass_pokemon"], err = trgGrassColl.CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		docCount["fire_pokemon"], err = trgFireColl.CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		if docCount["grass_pokemon"] == expectedCtGrass && docCount["fire_pokemon"] == expectedCtFire {
			break
		}
		time.Sleep(3 * time.Second)
	}

	require.Equal(t, expectedCtGrass, docCount["grass_pokemon"], "Wrong doc count in grass_pokemon in target")
	require.Equal(t, expectedCtFire, docCount["fire_pokemon"], "Wrong doc count in fire_pokemon in target")

	// Check that data have appeared in target
	var docResult bson.M
	err = trgGrassColl.FindOne(ctx, bulbasaur).Decode(&docResult)
	require.NoError(t, err, "No Bulbasaur in target :(")
	err = trgGrassColl.FindOne(ctx, ivysaur).Decode(&docResult)
	require.NoError(t, err, "No Ivysaur in target :(")
	err = trgFireColl.FindOne(ctx, charmander).Decode(&docResult)
	require.NoError(t, err, "No Charmander in target :(")
	err = trgFireColl.FindOne(ctx, charmeleon).Decode(&docResult)
	require.NoError(t, err, "No Charmeleon in target :(")

	// Both original dbs must be absent in target (contents must appear in renamed db)

	originalDB1 = targetClient.Database("original_db_1")
	res, err := originalDB1.ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	require.Len(t, res, 0)

	originalDB2 = targetClient.Database("original_db_2")
	res, err = originalDB2.ListCollectionNames(ctx, bson.D{})
	require.NoError(t, err)
	require.Len(t, res, 0)
}
