package permissiontest

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	cpclient "github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	mongocommon "github.com/transferria/transferria/pkg/providers/mongo"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	readOnlyDatabase  string = "read_only_db"
	technicalDatabase string = "data_transfer"
	collectionName    string = "collection"
)

var (
	port                 = helpers.GetIntFromEnv("ADMIN_MONGO_LOCAL_PORT")
	adminUserName        = os.Getenv("ADMIN_MONGO_LOCAL_USER")
	adminUserPassword    = os.Getenv("ADMIN_MONGO_LOCAL_PASSWORD")
	transferUserName     = os.Getenv("TRANSFER_USER_NAME")
	transferUserPassword = os.Getenv("TRANSFER_USER_PASSWORD")
)

func getSource(user, password string, collection ...mongocommon.MongoCollection) *mongocommon.MongoSource {
	return &mongocommon.MongoSource{
		Hosts:       []string{"localhost"},
		Port:        port,
		User:        user,
		Password:    model.SecretString(password),
		Collections: collection,
	}
}

var (
	adminUserSource = getSource(adminUserName, adminUserPassword)
)

func connect(source *mongocommon.MongoSource) (*mongocommon.MongoClientWrapper, error) {
	client, err := mongocommon.Connect(context.Background(), source.ConnectionOptions([]string{}), nil)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func makeReadOnlyUser(ctx context.Context, adminSource *mongocommon.MongoSource, userName, userPassword string) error {
	client, err := connect(adminSource)
	if err != nil {
		return err
	}
	defer client.Close(ctx)

	// https://mongoing.com/docs/reference/command/createUser.html#dbcmd.createUser
	//
	// db.runCommand("createUser", {createUser:"asdf", pwd:"kek", roles: [
	//    { role: "<role>", db: "<database>" } | "<role>",
	//    ...
	//  ],})
	//
	// db.runCommand("createUser", {createUser:"asdf", pwd:"kek", roles: ["read", {db: readWrite}]})
	cmdParams := bson.D{
		bson.E{Key: "createUser", Value: userName},
		bson.E{Key: "pwd", Value: userPassword},
		bson.E{Key: "roles", Value: bson.A{
			bson.D{
				bson.E{Key: "role", Value: "read"},
				bson.E{Key: "db", Value: readOnlyDatabase},
			},
			bson.D{
				bson.E{Key: "role", Value: "readWrite"},
				bson.E{Key: "db", Value: technicalDatabase},
			},
		}},
	}
	singleRes := client.Database("admin").RunCommand(ctx, cmdParams)
	if singleRes.Err() != nil {
		return singleRes.Err()
	}
	return nil
}

// TODO(@kry127) refactor doubles: https://github.com/transferria/transferria/arc_vcs/transfer_manager/go/pkg/worker/tasks/e2e/load_sharded_snapshot_test.go?rev=r9868991#L111
type permissionSinker struct {
	bannedCollections []mongocommon.MongoCollection
}

func (d permissionSinker) Close() error { return nil }
func (d permissionSinker) Push(items []abstract.ChangeItem) error {
	for _, item := range items {
		for _, bc := range d.bannedCollections {
			if bc.DatabaseName == item.Schema {
				if bc.CollectionName == item.Table || bc.CollectionName == "*" {
					return xerrors.Errorf("error: item should not be uploaded: %v", item)
				}
			}
		}
	}
	return nil
}

func makePermissionSinker(bannedCollections ...mongocommon.MongoCollection) *permissionSinker {
	return &permissionSinker{
		bannedCollections: bannedCollections,
	}
}

func snapshotAndIncrement(t *testing.T, ctx context.Context, source *mongocommon.MongoSource, permissionSinker *permissionSinker,
	sourceDB, collection string, expectError bool) {
	adminClient, err := connect(adminUserSource)
	require.NoError(t, err)
	defer adminClient.Close(ctx)

	//------------------------------------------------------------------------------------
	// insert one record

	adminDB := adminClient.Database(sourceDB)
	defer func() {
		// clear collection in the end (for local debug)
		_ = adminDB.Collection(collection).Drop(context.Background())
	}()
	err = adminDB.CreateCollection(context.Background(), collection)
	require.NoError(t, err)

	adminColl := adminDB.Collection(collection)

	type Myamlya struct {
		Name        string
		Age         int
		TableToDrop string
	}

	_, err = adminColl.InsertOne(context.Background(),
		Myamlya{"Eugene", 3, "connector_endpoints"})
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := model.Transfer{
		Type: abstract.TransferTypeSnapshotAndIncrement,
		Src:  source,
		Dst: &model.MockDestination{
			SinkerFactory: func() abstract.Sinker {
				return permissionSinker
			},
			Cleanup: model.Drop,
		},
		ID: helpers.TransferID,
	}

	accessErrorChecker := func(err error) {
		if expectError {
			require.Error(t, err, "error should happen: expected that user has not enough permission")
			expectedMessage := fmt.Sprintf("(Unauthorized) not authorized on %s to execute command", sourceDB)
			require.ErrorContainsf(t, err, expectedMessage,
				"error should be about unauthorized on source database. Expected message: %s", sourceDB)
		} else {
			msg := "expected, that user has permission to upload object, but got error: %v"
			require.NoError(t, err, fmt.Sprintf(msg, err))
		}
	}

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry())
	accessErrorChecker(err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// replicate one record

	_, err = adminColl.InsertOne(context.Background(), Myamlya{"Victor", 2, "public"})
	require.NoError(t, err)
}

func TestMongoPermissions(t *testing.T) {
	ctx := context.Background()

	err := makeReadOnlyUser(ctx, adminUserSource, transferUserName, transferUserPassword)
	expectedError := fmt.Sprintf("(Location51003) User \"%s@admin\" already exists", transferUserName)
	switch {
	case err != nil && err.Error() == expectedError: // OK
	default:
		require.NoError(t, err, "unable to create read-only user")
	}

	t.Run("AttemptToWriteToReadonlyTest", func(t *testing.T) {
		t.Skip("Skipped when fixing TM-4906, can be turned on again when auth will be working")
		src := getSource(transferUserName, transferUserPassword,
			mongocommon.MongoCollection{DatabaseName: readOnlyDatabase, CollectionName: collectionName},
		)
		dst := makePermissionSinker()
		snapshotAndIncrement(t, ctx, src, dst, readOnlyDatabase, collectionName, true)
	})

	t.Run("ReadFromReadOnlyWriteToTechnicalTest", func(t *testing.T) {
		src := getSource(transferUserName, transferUserPassword,
			mongocommon.MongoCollection{DatabaseName: readOnlyDatabase, CollectionName: collectionName},
		)
		src.TechnicalDatabase = technicalDatabase
		dst := makePermissionSinker(mongocommon.MongoCollection{DatabaseName: technicalDatabase, CollectionName: "*"})
		snapshotAndIncrement(t, ctx, src, dst, readOnlyDatabase, collectionName, false)
	})
	t.Run("ReadFromLegacyOplog", func(t *testing.T) {
		src := getSource(adminUserName, adminUserPassword,
			mongocommon.MongoCollection{DatabaseName: readOnlyDatabase, CollectionName: collectionName},
		)
		src.ReplicationSource = mongocommon.MongoReplicationSourceOplog
		dst := makePermissionSinker(mongocommon.MongoCollection{DatabaseName: mongocommon.DataTransferSystemDatabase, CollectionName: "*"})
		snapshotAndIncrement(t, ctx, src, dst, readOnlyDatabase, collectionName, false)
	})
	t.Run("ReadFromLegacyOplogOverrideDB", func(t *testing.T) {
		src := getSource(adminUserName, adminUserPassword,
			mongocommon.MongoCollection{DatabaseName: readOnlyDatabase, CollectionName: collectionName},
		)
		src.TechnicalDatabase = technicalDatabase
		src.ReplicationSource = mongocommon.MongoReplicationSourceOplog
		dst := makePermissionSinker(mongocommon.MongoCollection{DatabaseName: technicalDatabase, CollectionName: "*"})
		snapshotAndIncrement(t, ctx, src, dst, readOnlyDatabase, collectionName, false)
	})
}
