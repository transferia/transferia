package shmongo

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	mongoshardedconfig "github.com/transferia/transferia/recipe/mongo/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.ytsaurus.tech/library/go/core/log"
)

type ShardReplicaSet struct {
	ReplicaSet   string
	MongoDaemons []MongoD
}

func (c *ShardReplicaSet) Close() error {
	var errs util.Errors
	for _, mongod := range c.MongoDaemons {
		err := mongod.Close()
		errs = util.AppendErr(errs, err)
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

// LaunchMongoShardWithParams
// length of auxiliary `params` array equals mongod instances to be launched.
func LaunchMongoShard(
	logger log.Logger,
	binInfo EnvironmentInfo,
	config mongoshardedconfig.ReplicaSetConfig,
	defaultShardName string,
) (ShardReplicaSet, error) {
	if config.MongodConfigCount() == 0 {
		return ShardReplicaSet{}, xerrors.Errorf("try to launch config replica set with zero mongod instances specified")
	}
	ReplicaSet, err := InferReplicaSet(config, defaultShardName)
	if err != nil {
		return ShardReplicaSet{}, xerrors.Errorf("cannot infer replica set name from config: %w", err)
	}
	mongods, err := StartReplicaSet(logger, binInfo, config, ReplicaSet, MongoDShardSvr)
	if err != nil {
		return ShardReplicaSet{}, xerrors.Errorf("cannot start replica set: %w", err)
	}

	result := ShardReplicaSet{ReplicaSet: ReplicaSet, MongoDaemons: mongods}

	var rollbacks util.Rollbacks
	defer rollbacks.Do()
	rollbacks.Add(func() {
		err := result.Close()
		if err != nil {
			logger.Error("Unable to close config replica set in rollback", log.Error(err))
		}
	})

	if len(result.MongoDaemons) == 0 {
		return ShardReplicaSet{}, xerrors.Errorf("no mongod hosts to connect to")
	}
	mongod := result.MongoDaemons[0]
	err = mongod.WithRootConnection(func(client *mongo.Client) error {
		err := result.initShardReplicaSet(client)
		if err != nil {
			return xerrors.Errorf("cannot init shard replica set: %w", err)
		}
		return nil
	})
	if err != nil {
		return ShardReplicaSet{}, xerrors.Errorf("root context failed: %w", err)
	}

	rollbacks.Cancel()
	return result, nil
}

func (c *ShardReplicaSet) initShardReplicaSet(client *mongo.Client) error {
	// https://www.mongodb.com/docs/manual/reference/command/replSetInitiate/#mongodb-dbcommand-dbcmd.replSetInitiate
	// db.runCommand({replSetInitiate: {_id: "rs01", members: [{_id: 0, host: "localhost:56784"}, {_id: 1, host: "localhost:56785"}]}})
	var members bson.A
	for i, md := range c.MongoDaemons {
		members = append(members, bson.D{{Key: "_id", Value: i}, {Key: "host", Value: md.Fqdn()}})
	}
	res := client.Database("admin").RunCommand(context.TODO(), bson.D{
		{Key: "replSetInitiate", Value: bson.D{
			{Key: "_id", Value: c.ReplicaSet},
			{Key: "members", Value: members},
		}},
	})
	if res.Err() != nil {
		return res.Err()
	}
	return nil
}
