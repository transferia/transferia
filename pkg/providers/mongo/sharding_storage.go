//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	maxDelimiters = 31
)

type (
	ShardingFilter bson.D
	delimiter      interface {
		isDelimiter()
	}
)

func (*regularDelimiter) isDelimiter() {}
func (*infDelimiter) isDelimiter()     {}

type regularDelimiter struct {
	ID interface{}
}

func newDelimiter(ID interface{}) *regularDelimiter {
	return &regularDelimiter{
		ID: ID,
	}
}

type infDelimiter struct{}

func MarshalFilter(filter ShardingFilter) (string, error) {
	dataRangeFilter, err := bson.MarshalExtJSON(filter, true, false)
	if err != nil {
		return "", xerrors.Errorf("cannot marshal filter to ext json: %w", err)
	}
	return string(dataRangeFilter), nil
}

func UnmarshalFilter(marshalledFilter string) (ShardingFilter, error) {
	var filter ShardingFilter
	err := bson.UnmarshalExtJSON([]byte(marshalledFilter), true, &filter)
	if err != nil {
		return nil, xerrors.Errorf("cannot unmarshal filter from ext json: %w", err)
	}
	return filter, nil
}

func filterFromDelimiters(from, to delimiter) ShardingFilter {
	filter := emptyFilter
	// db.coll.find({_id: {$lte: ObjectId("61e7dca7ddc10da7cbaef46d")}})
	// db.coll.find({_id: {$gt: ObjectId("61e7dca7ddc10da7cbaef46d")}})
	// db.coll.find({_id: {$gt: ObjectId("61e7dca7ddc10da7cbaef46d"), $lte: ObjectId("61e7dd19ddc10da7cbaef46f")}})
	filterValues := bson.D{}
	if fromRegular, ok := from.(*regularDelimiter); ok {
		filterValues = append(filterValues, bson.E{Key: "$gt", Value: fromRegular.ID})
	}
	if toRegular, ok := to.(*regularDelimiter); ok {
		filterValues = append(filterValues, bson.E{Key: "$lte", Value: toRegular.ID})
	}
	if len(filterValues) > 0 {
		filter = append(filter, bson.E{Key: "_id", Value: filterValues})
	}
	return ShardingFilter(filter)
}

func filterFromTable(table abstract.TableDescription) (ShardingFilter, error) {
	filter := ShardingFilter(emptyFilter)

	if table.Filter != "" {
		var err error
		filter, err = UnmarshalFilter(string(table.Filter))
		if err != nil {
			return nil, xerrors.Errorf("cannot unmarshal filter of table description: %w", err)
		}
	}
	return filter, nil
}

// getRepresentativeFromEveryTypeBracket acquires representative from every type bracket
func getRepresentativeFromEveryTypeBracket(ctx context.Context, collection *mongo.Collection, isDocDB bool) ([]delimiter, error) {
	identifiers := []delimiter{}
	// user primitive.JavaScript instances and etc. instead of $type query like this:
	// > db.pays.find({$or: [{_id: {$lte: null}}, {_id: {$gte: null}}]})
	// > db.pays.find({$or: [{_id: {$lte: {}}}, {_id: {$gte: {}}}]})
	// > db.pays.find({$or: [{_id: {$lte: new ObjectId()}}, {_id: {$gte: new ObjectId()}}]})
	// this is much quicker query
	objects := []interface{}{
		1.27,
		"CSC",
		primitive.D{primitive.E{Key: "x", Value: 1}},
		// primitive.A{"a", "b", "c"}, // error: array is not applicable for ID
		primitive.Binary{Data: []byte("some data")},
		// primitive.Undefined{}, // error: cannot compare to undefined
		primitive.NewObjectID(),
		true,
		primitive.NewDateTimeFromTime(time.Now()),
		primitive.Null{},
		primitive.Timestamp{
			T: uint32(time.Now().Unix()),
			I: 0,
		},
		// primitive.Regex{Pattern: "asdf", Options: "i"}, // TODO(@kry127) how to turn on?
		// int32(3),                        // the same type bracket as 1.27
		// int64(293120039813945800),       // the same type bracket as 1.27
		// primitive.NewDecimal128(1, 1),   // the same type bracket as 1.27
	}
	if !isDocDB {
		objects = append(objects,
			primitive.DBPointer{
				DB:      "db",
				Pointer: primitive.NewObjectID(),
			},
			primitive.JavaScript("function() {}"),
			// primitive.Symbol("symbol"),     // the same type bracket as "CSC"
		)
	}
	for i, object := range objects {
		logger.Log.Info("Checking out object", log.Any("object", object), log.Any("iteration", i))
		var key bson.D
		err := collection.FindOne(ctx, bson.D{bson.E{Key: "$or", Value: bson.A{
			bson.D{bson.E{Key: "_id", Value: bson.D{bson.E{Key: "$lte", Value: object}}}},
			bson.D{bson.E{Key: "_id", Value: bson.D{bson.E{Key: "$gte", Value: object}}}},
		}}}, options.FindOne().SetAllowPartialResults(true)).Decode(&key)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				continue
			}
			return nil, xerrors.Errorf("error executing find one query: %w", err)
		}
		identifiers = append(identifiers, newDelimiter(key.Map()["_id"]))
	}
	return identifiers, nil
}

// getRandomIdentifiers returns desired amount of random document identifiers
func getRandomIdentifiers(ctx context.Context, collection *mongo.Collection, amount uint64) ([]delimiter, error) {
	// db.coll.aggregate([{ $sample: { size: 3 } }, { $sort : { _id : 1 }}])
	// if delimiter count is larger than collection, all documents will be returned in order
	pipeline := mongo.Pipeline{
		bson.D{bson.E{Key: "$sample", Value: bson.D{{Key: "size", Value: amount}}}},
		bson.D{bson.E{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		bson.D{bson.E{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}
	randDocsCursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, xerrors.Errorf("cannot select %d random documents: %w", amount, err)
	}
	defer randDocsCursor.Close(ctx)

	identifiers := []delimiter{}
	for randDocsCursor.Next(ctx) {
		var key bson.D
		if err := randDocsCursor.Decode(&key); err != nil {
			return nil, xerrors.Errorf("cannot decode cursor value: %w", err)
		}
		identifiers = append(identifiers, newDelimiter(key.Map()["_id"]))
	}
	if randDocsCursor.Err() != nil {
		return nil, xerrors.Errorf("cursor error occurred: %w", randDocsCursor.Err())
	}
	return identifiers, nil
}

// getDelimiters acquires delimiters that has representative in every type bracket
// and has desired amount of parts if it is possible
func getDelimiters(ctx context.Context, collection *mongo.Collection, amountOfDelimiters uint64, isDocDB bool) ([]delimiter, error) {
	typeBracketDelimiters, err := getRepresentativeFromEveryTypeBracket(ctx, collection, isDocDB)
	if err != nil {
		return nil, xerrors.Errorf("cannot get representatives from every type bracket: %w", err)
	}
	if len(typeBracketDelimiters) > 1 {
		logger.Log.Warn(
			"Sharding mechanism identified that sharding by '_id' field has more than one type bracket",
			log.Any("type_brackets_amount", len(typeBracketDelimiters)),
			log.Any("type_brackets_delimiters", typeBracketDelimiters),
		)
		// assume that there is only one type of delimiters
		return nil, abstract.NewNonShardableError(xerrors.Errorf("there are two or more types of objects in the sharding index"))
	}
	return getRandomIdentifiers(ctx, collection, amountOfDelimiters)
}

func (s Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	if table.Filter != "" || table.Offset != 0 {
		logger.Log.Infof("Table %v will not be sharded, filter: [%v], offset: %v", table.Fqtn(), table.Filter, table.Offset)
		return []abstract.TableDescription{table}, nil
	}
	mongoDatabase := s.Client.Database(table.Schema)
	mongoCollection := mongoDatabase.Collection(table.Name)
	tableSize, err := s.TableSizeInBytes(table.ID())
	if err != nil {
		return nil, xerrors.Errorf("cannot get table size in bytes: %w", err)
	}
	if s.desiredPartSize == 0 {
		return nil, abstract.NewNonShardableError(xerrors.Errorf("desired part size is inapplicable for sharding: %v", s.desiredPartSize))
	}
	delimiterCount := tableSize / s.desiredPartSize
	logger.Log.Info("ShardTable info",
		log.Any("desiredPartSize", s.desiredPartSize),
		log.Any("tableSize", tableSize),
		log.Any("delimiterCount", delimiterCount),
		log.Any("table", table.Fqtn()),
	)
	if delimiterCount == 0 {
		return []abstract.TableDescription{table}, nil
	}
	if delimiterCount > maxDelimiters {
		delimiterCount = maxDelimiters
	}

	logger.Log.Info("ShardTable pre-info capped delimiter count",
		log.Any("table", table.Fqtn()),
		log.Any("delimiterCount", delimiterCount),
	)

	result := []abstract.TableDescription{}
	addToRange := func(from, to delimiter) error {
		filter := filterFromDelimiters(from, to)

		marshalledFilter, err := MarshalFilter(filter)
		if err != nil {
			return xerrors.Errorf("cannot marshal filter: %w", err)
		}
		result = append(result, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.WhereStatement(marshalledFilter),
			EtaRow: 0,
			Offset: 0,
		})
		return nil
	}

	delimiters, err := getDelimiters(ctx, mongoCollection, delimiterCount, s.Client.IsDocDB)
	if err != nil {
		return nil, xerrors.Errorf("cannot get delimiters: %w", err)
	}
	delimiters = append(delimiters, new(infDelimiter))
	var lastDelimiter delimiter = new(infDelimiter)
	for _, d := range delimiters {
		if err := addToRange(lastDelimiter, d); err != nil {
			return nil, xerrors.Errorf("add range error occurred: %w", err)
		}
		lastDelimiter = d
	}

	logger.Log.Info("ShardTable info",
		log.Any("desiredPartSize", s.desiredPartSize),
		log.Any("tableSize", tableSize),
		log.Any("delimiterCount", delimiterCount),
		log.Any("table", table.Fqtn()),
		log.Any("cursorsCount", len(result)),
	)
	return result, nil
}
