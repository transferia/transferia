package shmongo

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/util"
	mongoshardedconfig "github.com/transferia/transferia/recipe/mongo/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.ytsaurus.tech/library/go/core/log"
)

type Cluster struct {
	MongoSList []MongoS
	ShardList  []ShardReplicaSet
	ConfigRS   ConfigReplicaSet
}

func (c Cluster) GetAllPids() ([]int, error) {
	var pids []int
	for _, mongos := range c.MongoSList {
		pid, err := extractPidFromPidFile(mongos.PidFilePath)
		if err != nil {
			return nil, xerrors.Errorf("error reading pid from file '%s': %w", mongos.PidFilePath, err)
		}
		pids = append(pids, pid)
	}
	for _, mongod := range c.ConfigRS.MongoDaemons {
		pid, err := extractPidFromPidFile(mongod.PidFilePath)
		if err != nil {
			return nil, xerrors.Errorf("error reading pid from file '%s': %w", mongod.PidFilePath, err)
		}
		pids = append(pids, pid)
	}
	for _, shard := range c.ShardList {
		for _, mongod := range shard.MongoDaemons {
			pid, err := extractPidFromPidFile(mongod.PidFilePath)
			if err != nil {
				return nil, xerrors.Errorf("error reading pid from file '%s': %w", mongod.PidFilePath, err)
			}
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

func StartCluster(logger log.Logger, envInfo EnvironmentInfo, config mongoshardedconfig.MongoShardedClusterConfig) (Cluster, error) {
	logger.Infof("Starting sharded mongodb cluster with %d shards", config.ShardReplicaSet.ReplicaSetCount())

	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	// launch config replica set
	configReplicaSet, err := LaunchConfigReplicaSet(logger, envInfo, config.ConfigReplicaSet)
	if err != nil {
		return Cluster{}, xerrors.Errorf("cannot start config replica set: %w", err)
	}
	rollbacks.Add(func() {
		err := configReplicaSet.Close()
		if err != nil {
			logger.Error("Unable to close config replica set in rollback", log.Error(err))
		}
	})

	// launch mongod instances for each shard
	shardReplicaSets := []ShardReplicaSet{}
	for i := uint(0); i < config.ShardReplicaSet.ReplicaSetCount(); i++ {
		shardCfg, err := config.ShardReplicaSet.GetReplicaSetConfig(i)
		if err != nil {
			return Cluster{}, xerrors.Errorf("cannot get shard replica set config: %w", err)
		}
		defaultShardName := fmt.Sprintf("rs%02d", i+1)
		shardReplicaSet, err := LaunchMongoShard(logger, envInfo, shardCfg, defaultShardName)
		if err != nil {
			return Cluster{}, xerrors.Errorf("cannot start shard #%d replica set: %w", i+1, err)
		}
		rollbacks.Add(func() {
			err := shardReplicaSet.Close()
			if err != nil {
				logger.Error("Unable to close shard replica set in rollback", log.Error(err))
			}
		})
		shardReplicaSets = append(shardReplicaSets, shardReplicaSet)
	}
	if len(shardReplicaSets) == 0 {
		return Cluster{}, xerrors.Errorf("no data shards replica sets were launched")
	}
	// launch mongos instance
	mongos, err := StartSingleMongos(logger, envInfo, configReplicaSet, shardReplicaSets, config.Mongos)
	if err != nil {
		return Cluster{}, xerrors.Errorf("error starting single mongos instance: %w", err)
	}
	rollbacks.Add(func() {
		err := mongos.Close()
		if err != nil {
			logger.Error("Unable to close shard replica set in rollback", log.Error(err))
		}
	})
	// perform post-actions from config
	err = mongos.WithRootConnection(func(client *mongo.Client) error {
		createAdminReq := config.PostSteps.CreateAdminUser
		if createAdminReq.User == "" && createAdminReq.Password == "" && createAdminReq.AuthSource == "" {
			// no need to create anything
			return nil
		}
		if createAdminReq.User == "" {
			return xerrors.Errorf("empty username in admin user description")
		}
		if createAdminReq.Password == "" {
			return xerrors.Errorf("empty password in admin user description")
		}
		if createAdminReq.AuthSource == "" {
			return xerrors.Errorf("empty auth source in admin user description")
		}
		r := client.Database(createAdminReq.AuthSource).RunCommand(context.TODO(),
			bson.D{
				{Key: "createUser", Value: createAdminReq.User},
				{Key: "pwd", Value: createAdminReq.Password},
				{Key: "roles", Value: bson.A{
					bson.D{{Key: "db", Value: "admin"}, {Key: "role", Value: "clusterAdmin"}},
				}},
			},
		)
		if r.Err() != nil {
			return xerrors.Errorf("unable to create admin user: %w", r.Err())
		}
		return nil
	})
	if err != nil {
		return Cluster{}, xerrors.Errorf("unable to perform post-actions when configuring sharded cluster: %w", err)
	}

	result := Cluster{
		MongoSList: []MongoS{mongos},
		ShardList:  shardReplicaSets,
		ConfigRS:   configReplicaSet,
	}
	err = saveAllPids(result, envInfo)
	if err != nil {
		return Cluster{}, xerrors.Errorf("error saving all pids: %w", err)
	}

	// return result
	logger.Infof("Done starting sharded mongodb cluster! Connect to mongos instance 'localhost:%d'", mongos.Port)
	rollbacks.Cancel()
	return result, nil
}

func saveAllPids(cluster Cluster, envInfo EnvironmentInfo) error {
	pids, err := cluster.GetAllPids()
	if err != nil {
		return xerrors.Errorf("cannot get all pids for cluster")
	}

	pidsFileContent := strings.Join(
		yslices.Map(pids, func(pid int) string { return fmt.Sprint(pid) }),
		"\n")
	return os.WriteFile(envInfo.PidFilePath(), []byte(pidsFileContent), 0755)
}

func StopCluster(logger log.Logger, envInfo EnvironmentInfo) error {
	data, err := os.ReadFile(envInfo.PidFilePath())
	if err != nil {
		return xerrors.Errorf("unable to read file: %w", err)
	}

	var multiErr util.Errors
	pidsAsStrings := strings.Split(string(data), "\n")
	for _, pidAsString := range pidsAsStrings {
		pidInt, err := extractPidFromString(pidAsString)
		if err != nil {
			multiErr = util.AppendErr(multiErr, err)
			continue
		}
		err = closeWithPidInt(pidInt)
		if err != nil {
			multiErr = util.AppendErr(multiErr, err)
			continue
		}
	}
	if !multiErr.Empty() {
		return multiErr
	}
	return nil
}
