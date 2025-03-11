package snapshot

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	mongostorage "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	DB1                    = "db1"
	DB2                    = "db2"
	CollectionGood         = "kry127_good"
	CollectionBsonTooLarge = "kry127_bson_too_large"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = mongostorage.MongoSource{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:     os.Getenv("MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Collections: []mongostorage.MongoCollection{
			{DatabaseName: DB1, CollectionName: CollectionGood},
			{DatabaseName: DB1, CollectionName: CollectionBsonTooLarge},
			{DatabaseName: DB2, CollectionName: "*"}, // this is almost the same
		},
	}
	Target = mongostorage.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("DB0_MONGO_LOCAL_PORT"),
		User:     os.Getenv("DB0_MONGO_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("DB0_MONGO_LOCAL_PASSWORD")),
		Cleanup:  model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

type KV struct {
	Key   string `bson:"_id"`
	Value string
}

const Alphabet = "abcdefghijklmnopqrstuvwxyz"

func randString(size int) string {
	ret := make([]byte, size)
	for i := range ret {
		ret[i] = Alphabet[int(rand.Uint32())%len(Alphabet)]
	}
	return string(ret)
}

func NewKV(keysize, valsize int) *KV {
	return &KV{Key: randString(keysize), Value: randString(valsize)}
}

//---------------------------------------------------------------------------------------------------------------------
// utils

func LogMongoSource(s *mongostorage.MongoSource) {
	fmt.Printf("Source.Hosts: %v\n", s.Hosts)
	fmt.Printf("Source.Port: %v\n", s.Port)
	fmt.Printf("Source.User: %v\n", s.User)
	fmt.Printf("Source.Password: %v\n", s.Password)
}

func LogMongoDestination(s *mongostorage.MongoDestination) {
	fmt.Printf("Target.Hosts: %v\n", s.Hosts)
	fmt.Printf("Target.Port: %v\n", s.Port)
	fmt.Printf("Target.User: %v\n", s.User)
	fmt.Printf("Target.Password: %v\n", s.Password)
}

func MakeDstClient(t *mongostorage.MongoDestination) (*mongostorage.MongoClientWrapper, error) {
	return mongostorage.Connect(context.Background(), t.ConnectionOptions([]string{}), nil)
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
		// Test two different modes.
		// NOTE: heavily dependent on mongo version, be careful with recipes
		t.Run("Load_FromChangeStream", LoadFromchangestream)
		t.Run("Load_FromPureCursor", LoadFrompurecursor)
		t.Run("Load_FromOplog", LoadFromoplog)
	})
}

func Ping(t *testing.T) {
	// ping src
	LogMongoSource(&Source)
	client, err := mongostorage.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	defer func() { _ = client.Close(context.Background()) }()
	require.NoError(t, err)
	err = client.Ping(context.TODO(), nil)
	require.NoError(t, err)

	// ping dst
	LogMongoDestination(&Target)
	client2, err := MakeDstClient(&Target)
	defer func() { _ = client2.Close(context.Background()) }()
	require.NoError(t, err)
	err = client2.Ping(context.TODO(), nil)
	require.NoError(t, err)
}

func clearSrc(t *testing.T, client *mongostorage.MongoClientWrapper) {
	t.Helper()
	var err error
	for _, dbName := range []string{DB1, DB2} {
		db := client.Database(dbName)
		_ = db.Collection(CollectionGood).Drop(context.Background())
		err = db.CreateCollection(context.Background(), CollectionGood)
		require.NoError(t, err)
		_ = db.Collection(CollectionBsonTooLarge).Drop(context.Background())
		err = db.CreateCollection(context.Background(), CollectionBsonTooLarge)
		require.NoError(t, err)
	}
}

func LoadFromchangestream(t *testing.T) {
	client, err := mongostorage.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	// recreate collections on source
	clearSrc(t, client)
	db1 := client.Database(DB1)
	coll1good := db1.Collection(CollectionGood)
	coll1toolarge := db1.Collection(CollectionBsonTooLarge)
	db2 := client.Database(DB2)
	coll2good := db2.Collection(CollectionGood)
	coll2toolarge := db2.Collection(CollectionBsonTooLarge)

	// wait a little bit for oplog to shake up
	time.Sleep(5 * time.Second) // TODO(@kry127) is it needed

	// start replication
	Source.ReplicationSource = mongostorage.MongoReplicationSourcePerDatabaseFullDocument // set fetch mode
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	errChan := make(chan error, 1)
	go func() {
		errChan <- localWorker.Run() // like .Start(), but we in control for processing error in test
	}()
	defer func() { _ = localWorker.Stop() }()

	// replicate good records
	dstStorage, err := mongostorage.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)

	var goodInsertionsSize uint64 = 2940
	goodInsertionsCount := 20
	for _, coll := range []*mongo.Collection{coll1good, coll2good, coll1toolarge, coll2toolarge} {
		for i := 0; i < goodInsertionsCount; i++ {
			_, err = coll.InsertOne(context.Background(), NewKV(20, 100))
			require.NoError(t, err)
		}
	}
	time.Sleep(time.Second)

	tryingsCount := 30
	tries := 0
	var dstTableSize uint64 = 0
	for tries = 0; tries < tryingsCount; tries++ {
		allOk := true
		for _, td := range []abstract.TableDescription{
			{Schema: DB1, Name: CollectionGood},
			{Schema: DB2, Name: CollectionGood},
			{Schema: DB1, Name: CollectionBsonTooLarge},
			{Schema: DB2, Name: CollectionBsonTooLarge},
		} {
			dstTableSize, err = dstStorage.TableSizeInBytes(td.ID())
			require.NoError(t, err)

			t.Logf("Table %s, calculating size. Expected %d, actual %d", td.String(), goodInsertionsSize, dstTableSize)
			if dstTableSize != goodInsertionsSize {
				allOk = false
				break
			}
		}
		if allOk {
			break
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	// insert large documents
	for _, coll := range []*mongo.Collection{coll1toolarge, coll2toolarge} {
		_, err = coll.InsertOne(context.Background(), NewKV(4*1024*1024, 10*1024*1024)) // should be processable
		require.NoError(t, err)
		_, err = coll.InsertOne(context.Background(), NewKV(5*1024*1024, 10)) // should fail in full document mode
		require.NoError(t, err)
	}

	// wait for appropriate error from replication
	timer := time.NewTimer(30 * time.Second)
	select {
	case err := <-errChan:
		require.True(t, abstract.IsFatal(err), "should be fatal")
		require.Contains(t, err.Error(), "BSONObjectTooLarge", "Error should be about too large object in oplog")
		containsProperName := strings.Contains(err.Error(), coll1toolarge.Name()) || strings.Contains(err.Error(), coll2toolarge.Name())
		require.True(t, containsProperName, "Should contain collection name")
	case <-timer.C:
		t.Fatal("Couldn't wait for error from worker")
	}
}

func LoadFrompurecursor(t *testing.T) {
	client, err := mongostorage.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	// recreate collections on source
	clearSrc(t, client)
	db1 := client.Database(DB1)
	coll1good := db1.Collection(CollectionGood)
	coll1toolarge := db1.Collection(CollectionBsonTooLarge)
	db2 := client.Database(DB2)
	coll2good := db2.Collection(CollectionGood)
	coll2toolarge := db2.Collection(CollectionBsonTooLarge)

	// wait a little bit for oplog to shake up
	time.Sleep(5 * time.Second) // TODO(@kry127) is it needed

	// start replication
	Source.ReplicationSource = mongostorage.MongoReplicationSourcePerDatabase
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer func() { _ = localWorker.Stop() }()

	// replicate good records
	dstStorage, err := mongostorage.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)

	const goodInsertionsCount = 20
	const goodInsertionsSize uint64 = 2940
	for _, coll := range []*mongo.Collection{coll1good, coll2good, coll1toolarge, coll2toolarge} {
		for i := 0; i < goodInsertionsCount; i++ {
			_, err = coll.InsertOne(context.Background(), NewKV(20, 100))
			require.NoError(t, err)
		}
	}
	time.Sleep(time.Second)

	const tryingsCount int = 30
	var dstTableSize uint64 = 0
	for tries := 0; tries < tryingsCount; tries++ {
		allOk := true
		for _, td := range []abstract.TableDescription{
			{Schema: DB1, Name: CollectionGood},
			{Schema: DB2, Name: CollectionGood},
			{Schema: DB1, Name: CollectionBsonTooLarge},
			{Schema: DB2, Name: CollectionBsonTooLarge},
		} {
			dstTableSize, err = dstStorage.TableSizeInBytes(td.ID())
			require.NoError(t, err)

			t.Logf("Table %s, calculating size. Expected %d, actual %d", td.String(), goodInsertionsSize, dstTableSize)
			if dstTableSize != goodInsertionsSize {
				allOk = false
				break
			}
		}
		if allOk {
			break
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	// insert large documents
	const badInsertionsSize uint64 = 19923008
	for _, coll := range []*mongo.Collection{coll1toolarge, coll2toolarge} {
		_, err = coll.InsertOne(context.Background(), NewKV(4*1024*1024, 10*1024*1024)) // should be processable
		require.NoError(t, err)
		_, err = coll.InsertOne(context.Background(), NewKV(5*1024*1024, 10)) // also should be processed with pure cursor
		require.NoError(t, err)
		//_, err = coll.InsertOne(context.Background(), NewKV(5*1024*1024 + 512 * 1024, 0)) // you shall not pass :D
		//require.NoError(t, err)
	}

	for tries := 0; tries < tryingsCount; tries++ {
		allOk := true
		for _, td := range []abstract.TableDescription{
			{Schema: DB1, Name: CollectionBsonTooLarge},
			{Schema: DB2, Name: CollectionBsonTooLarge},
		} {
			dstTableSize, err = dstStorage.TableSizeInBytes(td.ID())
			require.NoError(t, err)

			t.Logf("Table %s, calculating size. Expected %d, actual %d", td.String(), goodInsertionsSize+badInsertionsSize, dstTableSize)
			if dstTableSize != goodInsertionsSize+badInsertionsSize {
				allOk = false
				break
			}
		}
		if allOk {
			break
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func LoadFromoplog(t *testing.T) {
	client, err := mongostorage.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	// recreate collections on source
	clearSrc(t, client)
	db := client.Database(DB1)
	coll := db.Collection(CollectionBsonTooLarge)

	// start replication
	Source.ReplicationSource = mongostorage.MongoReplicationSourceOplog // set replication source
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer func() { _ = localWorker.Stop() }()

	// insert large documents
	_, err = coll.InsertOne(context.Background(), NewKV(4*1024*1024, 10*1024*1024)) // should be processable
	require.NoError(t, err)
	_, err = coll.InsertOne(context.Background(), NewKV(5*1024*1024, 10)) // also should be processed with pure cursor
	require.NoError(t, err)
	_, err = coll.InsertOne(context.Background(), NewKV(5*1024*1024+512*1024, 0)) // you shall not pass :D
	require.NoError(t, err)
	_, err = coll.InsertOne(context.Background(), NewKV(15*1024*1024, 30)) // ???
	require.NoError(t, err)
	// wait for large document insertion
	time.Sleep(5 * time.Second)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
