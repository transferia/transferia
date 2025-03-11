package util

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	mongoshardedcluster "github.com/transferria/transferria/recipe/mongo/pkg/cluster"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMongoShardedClusterRecipe(t *testing.T) {
	require.True(t, true, "should be true")
	for _, envVariable := range []string{
		mongoshardedcluster.EnvMongoShardedClusterHost,
		mongoshardedcluster.EnvMongoShardedClusterPort,
		mongoshardedcluster.EnvMongoShardedClusterUsername,
		mongoshardedcluster.EnvMongoShardedClusterPassword,
		mongoshardedcluster.EnvMongoShardedClusterAuthSource,
	} {
		_, ok := os.LookupEnv(envVariable)
		require.True(t, ok, fmt.Sprintf("environment variable %s should be published "+
			"after successfully started sharded mongo recipe", envVariable))
	}

	hostSpec := fmt.Sprintf("%s:%s",
		os.Getenv(mongoshardedcluster.EnvMongoShardedClusterHost),
		os.Getenv(mongoshardedcluster.EnvMongoShardedClusterPort),
	)
	client, err := mongo.NewClient(
		new(options.ClientOptions).
			SetHosts([]string{hostSpec}).
			SetAuth(options.Credential{
				AuthMechanism:           "",
				AuthMechanismProperties: nil,
				AuthSource:              os.Getenv(mongoshardedcluster.EnvMongoShardedClusterAuthSource),
				Username:                os.Getenv(mongoshardedcluster.EnvMongoShardedClusterUsername),
				Password:                os.Getenv(mongoshardedcluster.EnvMongoShardedClusterPassword),
				PasswordSet:             false,
			}),
	)
	require.NoError(t, err)
	err = client.Connect(context.Background())
	require.NoError(t, err)
	defer func() {
		err := client.Disconnect(context.TODO())
		require.NoError(t, err)
	}()

	require.NoError(t, client.Ping(context.Background(), nil))

	result, err := client.Database("test").Collection("test").InsertOne(context.Background(),
		bson.D{{Key: "hello", Value: "world!"}})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.InsertedID)

	sr := client.Database("test").Collection("test").FindOne(context.Background(),
		bson.D{{Key: "_id", Value: result.InsertedID}})
	require.NotNil(t, sr)
	var dest bson.M
	err = sr.Decode(&dest)
	require.NoError(t, err)
	require.Equal(t, dest["hello"], "world!")
}
