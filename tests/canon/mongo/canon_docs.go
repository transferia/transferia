package mongo

import (
	"context"
	"math"
	"time"

	mongocommon "github.com/transferria/transferria/pkg/providers/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func oid(hex string) primitive.ObjectID {
	res, err := primitive.ObjectIDFromHex(hex)
	if err != nil {
		panic(err)
	}
	return res
}

type IncrementUpdate struct{ Filter, Update interface{} }

var (
	SnapshotDocuments = []interface{}{
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12b7")}, {Key: "x", Value: 4}, {Key: "y", Value: 7}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12b8")}, {Key: "id", Value: bson.D{{Key: "z", Value: 2}, {Key: "incept", Value: bson.D{{Key: "y", Value: "asdf"}}}}}},
		bson.D{{Key: "_id", Value: bson.D{{Key: "x", Value: 0}, {Key: "y", Value: 1}}}},
		bson.D{{Key: "_id", Value: "omg"}},
		bson.D{{Key: "_id", Value: 8147}},
		bson.D{{Key: "_id", Value: int32(2499)}},
		bson.D{{Key: "_id", Value: int64(100502)}},
		bson.D{{Key: "_id", Value: true}},
		bson.D{{Key: "_id", Value: primitive.NewDecimal128(10101, 0)}},
		bson.D{{Key: "_id", Value: primitive.Timestamp{T: 1234526243, I: 1}}},
		bson.D{{Key: "_id", Value: primitive.NewDateTimeFromTime(time.Unix(848920, 4940200))}},
		bson.D{{Key: "_id", Value: primitive.Binary{Data: []byte("hello, world!")}}},
		bson.D{{Key: "_id", Value: primitive.Symbol("sex symbol")}},
		bson.D{{Key: "_id", Value: primitive.JavaScript("function(){ return \"omfg\" }")}, {Key: "what", Value: "is this?"}},
		bson.D{{Key: "_id", Value: primitive.Null{}}},
		bson.D{{Key: "_id", Value: primitive.DBPointer{DB: "dbdb", Pointer: oid("62e98ebcb28dbd2fffdf12b7")}}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12b9")}, {Key: "string", Value: "2354"}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c0")}, {Key: "int", Value: 402}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c1")}, {Key: "double", Value: 1.27}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c3")}, {Key: "timestamp", Value: primitive.Timestamp{
			T: 222,
			I: 1188888811,
		}}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c4")}, {Key: "nil", Value: nil}},
	}
	// this piece is valid only for repackable hetero transfer
	ExtraSnapshotDocuments = []interface{}{
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12b4")}, {Key: "nan", Value: math.NaN()}, {Key: "inf+", Value: math.Inf(1)}, {Key: "inf-", Value: math.Inf(-1)}},
	}
	IncrementDocuments = []interface{}{
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c5")}, {Key: "s", Value: 1}, {Key: "t", Value: 2}},
		bson.D{{Key: "_id", Value: bson.D{{Key: "u", Value: 8}, {Key: "v", Value: 9}}}},
		bson.D{{Key: "_id", Value: "woof"}},
		bson.D{{Key: "_id", Value: 123}},
		bson.D{{Key: "_id", Value: int32(8440)}},
		bson.D{{Key: "_id", Value: int64(100503)}},
		bson.D{{Key: "_id", Value: false}},
		bson.D{{Key: "_id", Value: primitive.NewDecimal128(10101, 1)}},
		bson.D{{Key: "_id", Value: primitive.Timestamp{T: 1234526243, I: 2}}},
		bson.D{{Key: "_id", Value: primitive.NewDateTimeFromTime(time.Unix(74271040, 412))}},
		bson.D{{Key: "_id", Value: primitive.Binary{Data: []byte("Привет, мир!")}}},
		bson.D{{Key: "_id", Value: primitive.Symbol("no homo 🍆")}},
		bson.D{{Key: "_id", Value: primitive.DBPointer{DB: "ddbb", Pointer: oid("62e98ebcb28dbd2fffdf12c4")}}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c6")}, {Key: "id", Value: "x"}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c7")}, {Key: "id", Value: oid("62e98ebcb28dbd2fffdf12ba")}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c8")}, {Key: "id", Value: primitive.JavaScript("function(){}")}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12c9")}, {Key: "decimal", Value: primitive.NewDecimal128(243994, 912994009199)}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12ca")}, {Key: "string", Value: "3"}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12cb")}, {Key: "int", Value: 5}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12cc")}, {Key: "double", Value: 2.56}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12ce")}, {Key: "timestamp", Value: primitive.Timestamp{
			T: 5555555,
			I: 333,
		}}},
		bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12cf")}, {Key: "nil", Value: primitive.Null{}}}, // primitive.Null is the same as nil
	}
	// Update existing
	IncrementUpdates = []IncrementUpdate{
		{
			Filter: bson.D{{Key: "_id", Value: "omg"}},
			Update: bson.D{{Key: "$set", Value: bson.D{{Key: "_id", Value: "omg"}, {Key: "omgval", Value: 44}}}},
		},
		{
			Filter: bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12b7")}},
			Update: bson.D{{Key: "$set", Value: bson.D{{Key: "_id", Value: oid("62e98ebcb28dbd2fffdf12b7")}, {Key: "x", Value: 29}}}},
		},
	}
)

func DropCollection(ctx context.Context, Source *mongocommon.MongoSource, databaseName string, collectionName string) error {
	client, err := mongocommon.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	if err != nil {
		return err
	}
	defer client.Close(ctx)
	collection := client.Database(databaseName).Collection(collectionName)
	if err := collection.Drop(ctx); err != nil {
		return err
	}
	return nil
}

func InsertDocs(ctx context.Context, Source *mongocommon.MongoSource, databaseName string, collectionName string, docs ...any) error {
	client, err := mongocommon.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	if err != nil {
		return err
	}
	defer client.Close(ctx)
	collection := client.Database(databaseName).Collection(collectionName)
	insertManyOpts := new(options.InsertManyOptions).
		SetBypassDocumentValidation(true)

	_, err = collection.InsertMany(ctx, docs, insertManyOpts)
	return err
}

func UpdateDocs(ctx context.Context, Source *mongocommon.MongoSource, databaseName string, collectionName string, docs ...IncrementUpdate) error {
	client, err := mongocommon.Connect(ctx, Source.ConnectionOptions([]string{}), nil)
	if err != nil {
		return err
	}
	defer client.Close(ctx)
	collection := client.Database(databaseName).Collection(collectionName)
	updateManyOpts := new(options.UpdateOptions).
		SetBypassDocumentValidation(true)
	for _, updDescr := range docs {
		_, err = collection.UpdateMany(ctx, updDescr.Filter, updDescr.Update, updateManyOpts)
		if err != nil {
			return err
		}
	}
	return nil
}
