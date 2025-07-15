package mongo

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/tests/tcrecipes"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const defaultUser = "root"
const defaultPassword = "password"
const defaultImage = "mongo:6"
const defaultRS = "rs01"
const defaultPort = nat.Port("27017/tcp")

type recipeOpts struct {
	prefix     string
	collection []MongoCollection
	database   string
}

type RecipeOption func(opts *recipeOpts)

func WithPrefix(prefix string) RecipeOption {
	return func(opts *recipeOpts) {
		opts.prefix = prefix
	}
}

func WithCollections(collections ...MongoCollection) RecipeOption {
	return func(opts *recipeOpts) {
		opts.collection = collections
	}
}

func WithDatabase(database string) RecipeOption {
	return func(opts *recipeOpts) {
		opts.database = database
	}
}

func RecipeSource(options ...RecipeOption) *MongoSource {
	var opts recipeOpts
	for _, opt := range options {
		opt(&opts)
	}
	if tcrecipes.Enabled() {
		container, err := StartMongoContainer(context.Background())
		if err != nil {
			panic(err)
		}
		initEnvs(opts, container)
	}
	return &MongoSource{
		ClusterID:              "",
		ConnectionID:           "",
		Hosts:                  []string{"localhost"},
		Port:                   GetIntFromEnv(opts.prefix + "MONGO_LOCAL_PORT"),
		ReplicaSet:             os.Getenv(opts.prefix + "MONGO_REPLICA_SET"),
		AuthSource:             "",
		User:                   os.Getenv(opts.prefix + "MONGO_LOCAL_USER"),
		Password:               model.SecretString(os.Getenv(opts.prefix + "MONGO_LOCAL_PASSWORD")),
		SRVMode:                false,
		Collections:            opts.collection,
		ExcludedCollections:    nil,
		SubNetworkID:           "",
		SecurityGroupIDs:       nil,
		TechnicalDatabase:      "",
		IsHomo:                 false,
		SlotID:                 "",
		SecondaryPreferredMode: false,
		TLSFile:                "",
		ReplicationSource:      "",
		BatchingParams:         nil,
		DesiredPartSize:        0,
		PreventJSONRepack:      false,
		FilterOplogWithRegexp:  false,
		Direct:                 os.Getenv(opts.prefix+"MONGO_LOCAL_DIRECT") == "1",
		RootCAFiles:            nil,
	}
}

func RecipeTarget(options ...RecipeOption) *MongoDestination {
	var opts recipeOpts
	for _, opt := range options {
		opt(&opts)
	}
	if tcrecipes.Enabled() {
		container, err := StartMongoContainer(context.Background())
		if err != nil {
			panic(err)
		}
		initEnvs(opts, container)
	}
	return &MongoDestination{
		ClusterID:         "",
		ConnectionID:      "",
		Hosts:             []string{"localhost"},
		Port:              GetIntFromEnv(opts.prefix + "MONGO_LOCAL_PORT"),
		Database:          opts.database,
		ReplicaSet:        os.Getenv(opts.prefix + "MONGO_REPLICA_SET"),
		AuthSource:        "",
		User:              os.Getenv(opts.prefix + "MONGO_LOCAL_USER"),
		Password:          model.SecretString(os.Getenv(opts.prefix + "MONGO_LOCAL_PASSWORD")),
		SRVMode:           false,
		TransformerConfig: nil,
		Cleanup:           "",
		SubNetworkID:      "",
		SecurityGroupIDs:  nil,
		TLSFile:           "",
		Direct:            os.Getenv(opts.prefix+"MONGO_LOCAL_DIRECT") == "1",
		RootCAFiles:       nil,
	}
}

func initEnvs(opts recipeOpts, container *MongoContainer) {
	_ = os.Setenv(opts.prefix+"MONGO_LOCAL_PORT", container.exposedPort.Port())
	_ = os.Setenv(opts.prefix+"MONGO_LOCAL_USER", container.user)
	_ = os.Setenv(opts.prefix+"MONGO_LOCAL_PASSWORD", container.password)
	_ = os.Setenv(opts.prefix+"MONGO_REPLICA_SET", defaultRS)
	_ = os.Setenv(opts.prefix+"MONGO_LOCAL_DIRECT", "1")
}

func GetIntFromEnv(varName string) int {
	val, err := strconv.Atoi(os.Getenv(varName))
	if err != nil {
		panic(err)
	}
	return val
}

// PostgresContainer represents the postgres container type used in the module
type MongoContainer struct {
	testcontainers.Container
	user        string
	password    string
	exposedPort nat.Port
}

func StartMongoContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*MongoContainer, error) {
	req := testcontainers.ContainerRequest{
		Image: defaultImage,
		Env: map[string]string{
			"MONGO_INITDB_DATABASE": "db",
		},
		ExposedPorts: []string{defaultPort.Port()},
		WaitingFor: wait.ForAll(
			wait.ForLog("Waiting for connections"),
			wait.ForListeningPort(defaultPort),
		),
		Cmd: []string{"mongod", "--replSet", defaultRS},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	for _, opt := range opts {
		_ = opt.Customize(&genericContainerReq)
	}
	if req.FromDockerfile.Dockerfile != "" {
		req.Image = ""
	}

	mongoContainer, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, xerrors.Errorf("unable to start container: %w", err)
	}

	user := defaultUser
	password := defaultPassword
	exposedPort, err := mongoContainer.MappedPort(ctx, defaultPort)
	if err != nil {
		return nil, xerrors.Errorf("unable to get port: %w", err)
	}
	uri := fmt.Sprintf("mongodb://localhost:%s/?directConnection=true", exposedPort.Port())
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, xerrors.Errorf("unable to connect to docker mongo: %w", err)
	}

	// init replica-set
	res := client.Database("admin").RunCommand(
		ctx,
		bson.M{"replSetInitiate": bson.M{
			"_id": defaultRS,
			"members": []bson.M{
				{"_id": 0, "host": fmt.Sprintf("localhost:%s", defaultPort.Port())},
			},
		}},
	)
	if res == nil {
		return nil, xerrors.Errorf("unable to run replSetInitiate, empty result")
	}
	if res.Err() != nil {
		return nil, xerrors.Errorf("unable to init replica set: %s", res.Err())
	}

	// create default user, may take some attempts due to replica-set reconfiguration
	if err := backoff.Retry(func() error {
		if res := client.Database("admin").RunCommand(
			ctx,
			bson.D{
				{Key: "createUser", Value: defaultUser},
				{Key: "pwd", Value: defaultPassword},
				{Key: "roles", Value: []bson.M{{"role": "clusterAdmin", "db": "admin"}}},
			},
		); res == nil || res.Err() != nil {
			return xerrors.New("unable to init user")
		}
		return nil
	}, backoff.NewExponentialBackOff()); err != nil {
		return nil, xerrors.Errorf("unable to init user: %w", err)
	}

	return &MongoContainer{Container: mongoContainer, password: password, user: user, exposedPort: exposedPort}, nil
}
