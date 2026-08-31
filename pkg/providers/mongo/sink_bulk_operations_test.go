package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"go.mongodb.org/mongo-driver/bson"
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
)

// retryTestSinker makes a sinker over the recipe mongo and a dropped collection with a unique index on "u".
func retryTestSinker(t *testing.T, ctx context.Context, collection string) (*sinker, *mongo_driver.Collection, Namespace) {
	target := RecipeTarget()
	client, err := Connect(ctx, target.ConnectionOptions(nil), logger.Log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	collID := Namespace{Database: "test_db", Collection: collection}
	coll := client.Database(collID.Database).Collection(collID.Collection)
	require.NoError(t, coll.Drop(ctx))
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	_, err = coll.Indexes().CreateOne(ctx, mongo_driver.IndexModel{
		Keys:    bson.D{{Key: "u", Value: 1}},
		Options: mongo_options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	return &sinker{
		client:  client,
		logger:  logger.Log,
		metrics: stats.NewSinkerStats(solomon.NewRegistry(nil)),
		config:  target,
	}, coll, collID
}

func upsertItem(id string, doc bson.D) abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  DocumentSchema.ColumnsNames,
		ColumnValues: []interface{}{id, doc},
		TableSchema:  DocumentSchema.Columns,
	}
}

func deleteItem(id string) abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:        abstract.DeleteKind,
		TableSchema: DocumentSchema.Columns,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{"_id"},
			KeyValues: []interface{}{id},
		},
	}
}

// The taker of the unique value comes before the freer: round one defers it, round two applies.
func TestRetryCollectionOrdered_UniqueValueMove(t *testing.T) {
	ctx := context.Background()
	s, coll, collID := retryTestSinker(t, ctx, "retry_unique_move")

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 1}}),
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}}),
	}
	require.NoError(t, s.retryCollectionOrdered(ctx, collID, items))

	var a, b bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "a"}).Decode(&a))
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "b"}).Decode(&b))
	_, aHasU := a["u"]
	require.False(t, aHasU)
	require.EqualValues(t, 1, b["u"])
}

// Ops of one document must not overtake its earlier deferred op: insert x is deferred by the unique
// conflict, so delete x waits with it instead of being applied first (and resurrecting x on retry).
func TestRetryCollectionOrdered_SameIDKeepsOrder(t *testing.T) {
	ctx := context.Background()
	s, coll, collID := retryTestSinker(t, ctx, "retry_same_id")

	_, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "c"}, {Key: "u", Value: 1}})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("x", bson.D{{Key: "_id", Value: "x"}, {Key: "u", Value: 1}}),
		deleteItem("x"),
		upsertItem("c", bson.D{{Key: "_id", Value: "c"}}),
	}
	require.NoError(t, s.retryCollectionOrdered(ctx, collID, items))

	require.ErrorIs(t, coll.FindOne(ctx, bson.M{"_id": "x"}).Err(), mongo_driver.ErrNoDocuments)
	var c bson.M
	require.NoError(t, coll.FindOne(ctx, bson.M{"_id": "c"}).Decode(&c))
	_, cHasU := c["u"]
	require.False(t, cHasU)
}

// A swap of unique values between two documents cannot be applied by single writes in any order.
func TestRetryCollectionOrdered_NoProgress(t *testing.T) {
	ctx := context.Background()
	s, coll, collID := retryTestSinker(t, ctx, "retry_no_progress")

	_, err := coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 1}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 2}},
	})
	require.NoError(t, err)

	items := []abstract.ChangeItem{
		upsertItem("a", bson.D{{Key: "_id", Value: "a"}, {Key: "u", Value: 2}}),
		upsertItem("b", bson.D{{Key: "_id", Value: "b"}, {Key: "u", Value: 1}}),
	}
	err = s.retryCollectionOrdered(ctx, collID, items)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot make progress")
}
