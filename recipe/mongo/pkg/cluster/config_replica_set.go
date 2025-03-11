package shmongo

import (
	"context"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/util"
	mongoshardedconfig "github.com/transferria/transferria/recipe/mongo/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	ConfigDefaultReplicaSetName = "rscfg"
)

type ConfigReplicaSet struct {
	ReplicaSet   string
	MongoDaemons []MongoD
}

func (c *ConfigReplicaSet) Close() error {
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

func LaunchConfigReplicaSet(
	logger log.Logger,
	binInfo EnvironmentInfo,
	config mongoshardedconfig.ReplicaSetConfig,
) (ConfigReplicaSet, error) {
	if config.MongodConfigCount() == 0 {
		return ConfigReplicaSet{}, xerrors.Errorf("try to launch config replica set with zero mongod instances specified")
	}
	ReplicaSet, err := InferReplicaSet(config, ConfigDefaultReplicaSetName)
	if err != nil {
		return ConfigReplicaSet{}, xerrors.Errorf("cannot infer replica set name from config: %w", err)
	}
	mongods, err := StartReplicaSet(logger, binInfo, config, ReplicaSet, MongoDConfigSvr)
	if err != nil {
		return ConfigReplicaSet{}, xerrors.Errorf("cannot start replica set: %w", err)
	}

	result := ConfigReplicaSet{ReplicaSet: ReplicaSet, MongoDaemons: mongods}

	var rollbacks util.Rollbacks
	defer rollbacks.Do()
	rollbacks.Add(func() {
		err := result.Close()
		if err != nil {
			logger.Error("Unable to close config replica set in rollback", log.Error(err))
		}
	})

	if len(result.MongoDaemons) == 0 {
		return ConfigReplicaSet{}, xerrors.Errorf("no mongod hosts to connect to")
	}
	mongod := result.MongoDaemons[0]
	err = mongod.WithRootConnection(func(client *mongo.Client) error {
		err := result.initConfigReplicaSet(client)
		if err != nil {
			return xerrors.Errorf("cannot init config replica set: %w", err)
		}
		return nil
	})
	if err != nil {
		return ConfigReplicaSet{}, xerrors.Errorf("root context failed: %w", err)
	}

	rollbacks.Cancel()
	return result, nil
}

func (c *ConfigReplicaSet) initConfigReplicaSet(client *mongo.Client) error {
	// https://www.mongodb.com/docs/manual/reference/command/replSetInitiate/#mongodb-dbcommand-dbcmd.replSetInitiate
	var members bson.A
	for i, md := range c.MongoDaemons {
		members = append(members, bson.D{{Key: "_id", Value: i}, {Key: "host", Value: md.Fqdn()}})
	}
	client.Database("admin").RunCommand(context.TODO(), bson.D{
		{Key: "replSetInitiate", Value: bson.D{
			{Key: "_id", Value: c.ReplicaSet},
			{Key: "configsvr", Value: true},
			{Key: "members", Value: members},
		}},
	})
	return nil
}
