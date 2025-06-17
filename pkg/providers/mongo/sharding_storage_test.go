//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MongoDocumentFactory func(int) bson.D

type CheckParts int

var (
	ExpectNothing         CheckParts = 0
	ExpectMoreThanOnePart CheckParts = 1
)

func makeStorage(
	t *testing.T,
	ctx context.Context,
	collection *abstract.TableID,
	totalDocuments int,
	documentFactory MongoDocumentFactory,
) (storage *Storage, close context.CancelFunc) {
	source := RecipeSource()
	source.SlotID = "mongo-go-brrr"
	source.WithDefaults()
	require.NotEqual(t, 0, source.DesiredPartSize)
	storage, err := NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	storage.desiredPartSize = 100000

	client, err := Connect(context.Background(), source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	coll := client.Database(collection.Namespace).Collection(collection.Name)
	require.NoError(t, coll.Drop(ctx))
	docs := []interface{}{}
	for i := 0; i < totalDocuments; i++ {
		docs = append(docs, documentFactory(i)) // approx 4 bytes
	}
	insertRes, err := coll.InsertMany(ctx, docs)
	require.NoError(t, err)
	require.Len(t, insertRes.InsertedIDs, len(docs), "appeared less documents than expected")
	return storage, func() {
		require.NoError(t, coll.Drop(ctx))
	}
}

func TestShardingStorage_ShardTable(t *testing.T) {
	testShardTablePositive(t, "IntAsKey", func(i int) bson.D {
		return bson.D{{Key: "_id", Value: i}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "IntAsValue", func(i int) bson.D {
		return bson.D{{Key: "x", Value: i}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "ObjectIDAsKey", func(i int) bson.D {
		return bson.D{{Key: "_id", Value: primitive.NewObjectID()}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "CompositeStructureAsKey", func(i int) bson.D {
		return bson.D{{Key: "_id", Value: bson.D{{Key: "z", Value: i}}}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "TimestampAsKey", func(i int) bson.D {
		return bson.D{{Key: "_id", Value: time.Now().Add(time.Second * time.Duration(i))}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "SymbolAsKey", func(i int) bson.D {
		return bson.D{{
			Key:   "_id",
			Value: primitive.Symbol(fmt.Sprintf("джуниортварь #%d", i)),
		}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "JavaScriptAsKey", func(i int) bson.D {
		return bson.D{{
			Key:   "_id",
			Value: primitive.JavaScript(fmt.Sprintf("function() {return \"миддлтварь #%d\"}", i)),
		}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "StringAsKey", func(i int) bson.D {
		return bson.D{{Key: "_id", Value: fmt.Sprintf("сеньортварь %d", i)}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "BinaryAsKey", func(i int) bson.D {
		return bson.D{{Key: "_id", Value: []byte(fmt.Sprintf("бинарная личность %d", i))}}
	}, 100000, ExpectMoreThanOnePart)
	testShardTablePositive(t, "NullAsKey", func(i int) bson.D {
		return bson.D{{Key: "_id", Value: primitive.Null{}}}
	}, 1, ExpectNothing)
	testShardTablePositive(t, "BoolAsKey", func(i int) bson.D {
		return bson.D{{Key: "_id", Value: i&1 == 1}}
	}, 2, ExpectNothing)
	testShardTableNegative(t, "HeterogeneousAsKey", func(i int) bson.D {
		var genRandom func(int) bson.D
		genRandom = func(lvl int) bson.D {
			cases := 6
			if lvl <= 0 {
				cases = 5
			}
			switch rand.Int() % cases {
			case 0:
				return bson.D{{Key: "_id", Value: i}}
			case 1:
				return bson.D{{Key: "x", Value: i}}
			case 2:
				return bson.D{{Key: "_id", Value: bson.D{{Key: "z", Value: i}}}}
			case 3:
				return bson.D{{Key: "_id", Value: time.Now().Add(time.Second * time.Duration(i))}}
			case 4:
				return bson.D{{Key: "_id", Value: fmt.Sprintf("%d", i)}}
			case 5:
				return bson.D{
					{Key: "_id", Value: bson.D{
						{Key: "a", Value: i},
						{Key: "b", Value: genRandom(lvl - 1)},
					}},
				}
			}
			return nil
		}

		return genRandom(2)
	})
}

func testShardTablePositive(
	t *testing.T,
	testName string,
	documentFactory MongoDocumentFactory,
	totalDocuments int,
	checkParts CheckParts,
) {
	t.Run(testName, func(t *testing.T) {
		collectionID := &abstract.TableID{
			Namespace: "db",
			Name:      fmt.Sprintf("__test_coll_%s", testName),
		}

		ctx := context.Background()
		storage, closeFunc := makeStorage(t, ctx, collectionID, totalDocuments, documentFactory)
		defer closeFunc()

		tables, err := storage.ShardTable(ctx, abstract.TableDescription{
			Name:   collectionID.Name,
			Schema: collectionID.Namespace,
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		})
		require.NoError(t, err)
		if checkParts == ExpectMoreThanOnePart {
			require.True(t, len(tables) > 1, "test expects more than one part")
		}
		var res []abstract.ChangeItem
		for _, tbl := range tables {
			require.NoError(t, storage.LoadTable(ctx, tbl, func(input []abstract.ChangeItem) error {
				for _, row := range input {
					if row.IsRowEvent() {
						res = append(res, row)
					}
				}
				return nil
			}))
		}
		require.Len(t, res, totalDocuments)
	})
}

func testShardTableNegative(t *testing.T, testName string, documentFactory MongoDocumentFactory) {
	t.Run(testName, func(t *testing.T) {
		collectionID := &abstract.TableID{
			Namespace: "db",
			Name:      fmt.Sprintf("__test_coll_%s", testName),
		}
		totalDocuments := 100000

		ctx := context.Background()
		storage, closeFunc := makeStorage(t, ctx, collectionID, totalDocuments, documentFactory)
		defer closeFunc()

		_, err := storage.ShardTable(ctx, abstract.TableDescription{
			Name:   collectionID.Name,
			Schema: collectionID.Namespace,
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "cannot get delimiters")
	})
}
