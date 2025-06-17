//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.mongodb.org/mongo-driver/bson"
	"go.ytsaurus.tech/library/go/core/log"
)

func must(version semver.Version, err error) semver.Version {
	if err != nil {
		panic(err)
	}
	return version
}

var MongoVersion4_0 = must(semver.ParseTolerant("4.0"))

// GetVersion tries to get version from database authSource. If authSource is empty string, default will be used
func GetVersion(ctx context.Context, client *MongoClientWrapper, authSource string) (*semver.Version, error) {
	if authSource == "" {
		authSource = DefaultAuthSource
	}

	if client == nil {
		return nil, nil
	}
	// https://stackoverflow.com/questions/67479826/how-to-get-the-version-of-mongodb-by-mongo-go-driver
	var commandResult bson.D
	command := bson.D{{Key: "buildInfo", Value: 1}}
	if err := client.Database(authSource).RunCommand(ctx, command).Decode(&commandResult); err != nil {
		return nil, xerrors.Errorf("error getting mongodb version from database \"%s\": %w", authSource, err)
	}
	versionAsString := fmt.Sprintf("%v", commandResult.Map()["version"])

	if mongoVersion, err := semver.ParseTolerant(versionAsString); err != nil {
		return nil, xerrors.Errorf("couldn't parse version '%v': %w", versionAsString, err)
	} else {
		return &mongoVersion, nil
	}
}

func ReplicationSourceFallback(logger log.Logger, pl MongoReplicationSource, mongoVersion *semver.Version) MongoReplicationSource {
	if mongoVersion.LT(MongoVersion4_0) {
		logger.Info("Implicit fallback to oplog because of mongo version too old",
			log.String("original_replication_source", string(pl)),
			log.Any("mongo_version", mongoVersion))
		return MongoReplicationSourceOplog
	}
	return pl
}
