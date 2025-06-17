//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ParallelizationUnit interface {
	fmt.Stringer
	Ping(ctx context.Context, client *MongoClientWrapper) error
	GetClusterTime(ctx context.Context, client *MongoClientWrapper) (*primitive.Timestamp, error)
	SaveClusterTime(ctx context.Context, client *MongoClientWrapper, timestamp *primitive.Timestamp) error
}
