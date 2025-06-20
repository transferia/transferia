//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"math"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func castTableSizeFloatNumberToUnt64(x float64) (uint64, error) {
	if math.IsNaN(x) {
		return 0, xerrors.Errorf("float is NaN")
	}
	if x < 0 {
		return 0, xerrors.Errorf("float number is negative: %f", x)
	}
	if x > float64(math.MaxUint64) {
		return 0, xerrors.Errorf("float number exceeds maximum of uint64")
	}
	// round to nearest int
	return uint64(math.Round(x)), nil
}

func castTableSizeNumberToUint64(x interface{}) (uint64, error) {
	switch x := x.(type) {
	case int:
		return uint64(x), nil
	case int8:
		return uint64(x), nil
	case int16:
		return uint64(x), nil
	case int32:
		return uint64(x), nil
	case int64:
		return uint64(x), nil
	case uint:
		return uint64(x), nil
	case uint8:
		return uint64(x), nil
	case uint16:
		return uint64(x), nil
	case uint32:
		return uint64(x), nil
	case uint64:
		return x, nil
	case float32:
		if res, err := castTableSizeFloatNumberToUnt64(float64(x)); err != nil {
			return res, xerrors.Errorf("[float32 path] %w", err)
		} else {
			return res, nil
		}
	case float64:
		return castTableSizeFloatNumberToUnt64(x)
	default:
		return 0, xerrors.Errorf("can't convert object %v to uint64.", x)
	}
}

//nolint:descriptiveerrors
func (s *Storage) TableSizeInBytes(table abstract.TableID) (uint64, error) {
	ctx := context.Background()

	rawR := s.Client.Database(table.Namespace).RunCommand(
		ctx,
		bson.D{{Key: "collStats", Value: table.Name}},
	)
	if rawR.Err() != nil {
		logger.Log.Warnf("failed to calculate table size for table %v: %v", table.Name, rawR.Err().Error())
		return 0, rawR.Err()
	}

	var decodedR bson.M
	if err := rawR.Decode(&decodedR); err != nil {
		logger.Log.Warnf("failed to calculate table size for table %v at decoding: %v", table.Name, err.Error())
		return 0, err
	}

	result, ok := decodedR["size"]
	if !ok {
		err := xerrors.Errorf("failed to calculate table size for table %v at obtaining result: 'size' is missing in result: %v", table.Name, decodedR)
		logger.Log.Warnf(err.Error())
		return 0, err
	}

	if resultInt, err := castTableSizeNumberToUint64(result); err != nil {
		return 0, xerrors.Errorf("failed to calculate table size for table %v at type casting: expected int32, got in result['size']: %v. Result: %v. Error: %w", table.Name, result, decodedR, err)
	} else {
		return resultInt, nil
	}
}

//nolint:descriptiveerrors
func (s *Storage) LoadTopBottomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	ctx := context.Background()
	st := util.GetTimestampFromContextOrNow(ctx)
	coll := s.Client.Database(table.Schema).Collection(table.Name)

	for _, v := range []int{-1, 1} {

		// no cursor timeout is unavailable for lower price tiers in atlas
		// https://www.mongodb.com/docs/atlas/reference/free-shared-limitations/
		findOptions := options.Find().SetMaxTime(606461538 * time.Millisecond)

		optionsObj := findOptions.SetSort(bson.D{{Key: "_id", Value: v}}).SetLimit(1000)
		cursor, err := coll.Find(ctx, emptyFilter, optionsObj)
		if err != nil {
			return err
		}

		err = s.readRowsAndPushByChunks(
			ctx,
			cursor,
			st,
			table,
			chunkSize,
			chunkByteSize,
			pusher,
		)

		_ = cursor.Close(ctx)

		if err != nil {
			return err
		}
	}

	return nil
}

//nolint:descriptiveerrors
func (s *Storage) LoadRandomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	ctx := context.Background()
	st := util.GetTimestampFromContextOrNow(ctx)
	coll := s.Client.Database(table.Schema).Collection(table.Name)

	group := bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: 2000}}}}
	optionsObj := options.Aggregate()
	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{group}, optionsObj)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close(ctx) }()

	return s.readRowsAndPushByChunks(
		ctx,
		cursor,
		st,
		table,
		chunkSize,
		chunkByteSize,
		pusher,
	)
}

func (s *Storage) LoadSampleBySet(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
	ctx := context.Background()
	st := util.GetTimestampFromContextOrNow(ctx)
	coll := s.Client.Database(table.Schema).Collection(table.Name)
	ids := make([]interface{}, 0)
	for _, m := range keySet {
		key, err := ExtractKey(m["_id"], s.IsHomo)
		if err != nil {
			return xerrors.Errorf("cannot extract key: %w", err)
		}
		ids = append(ids, key)
	}
	cursor, err := coll.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return xerrors.Errorf("coll.Find returned error: %w", err)
	}
	return s.readRowsAndPushByChunks(
		ctx,
		cursor,
		st,
		table,
		chunkSize,
		chunkByteSize,
		pusher,
	)
}

func (s *Storage) TableAccessible(table abstract.TableDescription) bool {
	coll := s.Client.Database(table.Schema).Collection(table.Name)
	ctx := context.TODO()
	sr := coll.FindOne(ctx, emptyFilter)

	if err := sr.Err(); err != nil && err != mongo.ErrNoDocuments {
		logger.Log.Warnf("Inaccessible table %v: %v", table.Fqtn(), err)
		return false
	}
	return true
}
