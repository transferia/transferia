package reorder

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	mongocommon "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

func makeSource(t *testing.T, database, collection string) *mongocommon.MongoSource {
	return &mongocommon.MongoSource{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:     helpers.GetEnvOfFail(t, "MONGO_LOCAL_USER"),
		Password: model.SecretString(helpers.GetEnvOfFail(t, "MONGO_LOCAL_PASSWORD")),
		Collections: []mongocommon.MongoCollection{
			{DatabaseName: database, CollectionName: collection},
		},
		BatchingParams: &mongocommon.BatcherParameters{
			BatchSizeLimit:     mongocommon.DefaultBatchSizeLimit,
			KeySizeThreshold:   mongocommon.DefaultKeySizeThreshold,
			BatchFlushInterval: mongocommon.DefaultBatchFlushInterval,
		},
	}
}

func makeTarget(t *testing.T, targetDatabase string) *mongocommon.MongoDestination {
	return &mongocommon.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		User:     helpers.GetEnvOfFail(t, "MONGO_LOCAL_USER"),
		Password: model.SecretString(helpers.GetEnvOfFail(t, "MONGO_LOCAL_PASSWORD")),
		Database: targetDatabase,
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz"

func randString(size int) string {
	ret := make([]byte, size)
	for i := range ret {
		ret[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(ret)
}

func makeBson() bson.D {
	result := bson.D{}
	keyCount := 2 + rand.Intn(3)
	ids := []int{}
	for i := 0; i < len(alphabet); i++ {
		ids = append(ids, i)
	}
	yslices.Shuffle(ids, rand.NewSource(time.Now().Unix()))
	for i := 0; i < keyCount; i++ {
		result = append(result, bson.E{Key: string(alphabet[ids[i]]), Value: randString(int(rand.Uint32()%4 + 2))})
	}
	return result
}

type BsonAsID struct {
	Key   bson.D `bson:"_id"`
	Value string `bson:"value"`
}

type BsonAsIndex struct {
	Index bson.D `bson:"index"`
	Value string `bson:"value"`
}

func bsonsEqual(a, b bson.D) int {
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}

	for i := range a {
		if a[i].Key < b[i].Key {
			return -1
		}
		if a[i].Key > b[i].Key {
			return 1
		}

		// assume values are string-only =)
		if a[i].Value.(string) < b[i].Value.(string) {
			return -1
		}
		if a[i].Value.(string) > b[i].Value.(string) {
			return 1
		}
	}
	return 0
}

func bsonPermEqual(a, b bson.D) bool {
	aM := a.Map()
	bM := b.Map()
	checkXincludedInY := func(x, y map[string]interface{}) bool {
		for keyX, valX := range x {
			valY, ok := y[keyX]
			if !ok || valX != valY {
				return false
			}
		}
		return true
	}
	if !checkXincludedInY(aM, bM) {
		return false
	}
	if !checkXincludedInY(bM, aM) {
		return false
	}
	return true
}

func bsonASubB(a []bson.D, b []bson.D) []interface{} {
	res := []interface{}{}
	for _, aa := range a {
		hasEqual := false
		for _, bb := range b {
			if bsonsEqual(aa, bb) == 0 || bsonPermEqual(aa, bb) {
				hasEqual = true
			}
		}
		if !hasEqual {
			res = append(res, aa)
		}
	}
	return res
}

func fetchPermutations(a []bson.D, b []bson.D) []interface{} {
	res := []interface{}{}
	for _, aa := range a {
		hasPerm := false
		for _, bb := range b {
			if bsonsEqual(aa, bb) != 0 && bsonPermEqual(aa, bb) {
				hasPerm = true
			}
		}
		if hasPerm {
			res = append(res, aa)
		}
	}
	return res
}

type collectionGenerator func(int) []interface{}

func generateBsonAsID(amount int) []interface{} {
	var documents []interface{}
	for i := 0; i < amount; i++ {
		documents = append(documents, BsonAsID{
			Key:   makeBson(),
			Value: randString(2),
		})
	}
	return documents
}

func generateBsonAsIndex(amount int) []interface{} {
	var documents []interface{}
	for i := 0; i < amount; i++ {
		documents = append(documents, BsonAsIndex{
			Index: makeBson(),
			Value: randString(2),
		})
	}
	return documents
}

type transferStage func(t *testing.T, inserter func() uint64, transfer *model.Transfer, targetDatabase, targetCollection string)

func snapshotOnlyStage(t *testing.T, inserter func() uint64, transfer *model.Transfer, _, _ string) {
	_ = inserter()

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	snapshotLoader := tasks.NewSnapshotLoader(cpclient.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)
}

func replicationOnlyStage(t *testing.T, inserter func() uint64, transfer *model.Transfer, targetDatabase, targetCollection string) {
	err := tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	amount := inserter()

	err = helpers.WaitDestinationEqualRowsCount(targetDatabase, targetCollection, helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, amount)
	require.NoError(t, err)
}

func TestBsonOrdering(t *testing.T) {
	t.Run("Test ID snapshot only", mkBsonTester("snapshot_id", generateBsonAsID, snapshotOnlyStage, abstract.TransferTypeSnapshotOnly).RunTest)
	t.Run("Test Index snapshot only", mkBsonTester("snapshot_index", generateBsonAsIndex, snapshotOnlyStage, abstract.TransferTypeSnapshotOnly).RunTest)
	t.Run("Test ID replication only", mkBsonTester("replication_id", generateBsonAsID, replicationOnlyStage, abstract.TransferTypeIncrementOnly).RunTest)
	t.Run("Test Index replication only", mkBsonTester("replication_index", generateBsonAsIndex, replicationOnlyStage, abstract.TransferTypeIncrementOnly).RunTest)
}

type bsonOrderingTester struct {
	collGenerator collectionGenerator
	stage         transferStage
	trType        abstract.TransferType

	collectionName string
}

func mkBsonTester(collectionName string,
	collGenerator collectionGenerator,
	stage transferStage,
	trType abstract.TransferType,
) *bsonOrderingTester {
	return &bsonOrderingTester{
		collGenerator:  collGenerator,
		stage:          stage,
		trType:         trType,
		collectionName: collectionName,
	}
}

func (b *bsonOrderingTester) RunTest(t *testing.T) {
	ctx := context.Background()

	sourceDB := "tm3500"
	targetDB := fmt.Sprintf("%s_d", sourceDB)
	src := makeSource(t, sourceDB, b.collectionName)
	dst := makeTarget(t, targetDB)

	sourceClient, err := mongocommon.Connect(context.Background(), src.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	targetClient, err := mongocommon.Connect(context.Background(), dst.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)

	mongoSourceCollection := sourceClient.Database(sourceDB).Collection(b.collectionName)
	mongoTargetCollection := targetClient.Database(targetDB).Collection(b.collectionName)

	_ = mongoSourceCollection.Drop(ctx)
	_ = mongoTargetCollection.Drop(ctx)

	err = sourceClient.Database(sourceDB).CreateCollection(ctx, b.collectionName)
	require.NoError(t, err)

	documents := b.collGenerator(20)

	tr := helpers.MakeTransfer("dttztm3500ztestzid", src, dst, b.trType)
	b.stage(t, func() uint64 {
		res, err := mongoSourceCollection.InsertMany(ctx, documents)
		require.NoError(t, err)
		return uint64(len(res.InsertedIDs))
	}, tr, targetDB, b.collectionName)

	cursor, err := mongoTargetCollection.Find(ctx, bson.D{})
	require.NoError(t, err)

	var newDocuments []bson.D
	err = cursor.All(ctx, &newDocuments)
	require.NoError(t, err)

	keyAsID, indexAsID := false, false
	docsToBsons := yslices.Map(documents, func(doc interface{}) bson.D {
		switch d := doc.(type) {
		case BsonAsID:
			keyAsID = true
			return d.Key
		case BsonAsIndex:
			indexAsID = true
			return d.Index
		default:
			t.Fatalf("unexpected type of document: '%T'", doc)
			return nil
		}
	})
	if keyAsID && indexAsID {
		t.Fatalf("Collection of heterogeneous type! choose only one of them")
	}
	newDocumentsToBsons := yslices.Map(newDocuments, func(doc bson.D) bson.D {
		if keyAsID {
			return doc.Map()["_id"].(bson.D)
		}
		if indexAsID {
			return doc.Map()["index"].(bson.D)
		}
		t.Fatalf("Illegal: collection should have certain type")
		return nil
	})

	// compare slices
	perms := fetchPermutations(docsToBsons, newDocumentsToBsons)
	lhsMiss := bsonASubB(docsToBsons, newDocumentsToBsons)
	rhsMiss := bsonASubB(newDocumentsToBsons, docsToBsons)

	if len(perms) > 0 {
		t.Errorf("%d permutations found: %v", len(perms), perms)
	}
	if len(lhsMiss) > 0 {
		t.Errorf("%d documents dropped during transfer: %v", len(lhsMiss), lhsMiss)
	}
	if len(rhsMiss) > 0 {
		t.Errorf("%d documents appeared during transfer: %v", len(rhsMiss), rhsMiss)
	}
	require.Empty(t, perms, "some documents shuffled fields during transfer")
	require.Empty(t, lhsMiss, "some documents dropped during transfer")
	require.Empty(t, rhsMiss, "some documents appeared during transfer")
}
