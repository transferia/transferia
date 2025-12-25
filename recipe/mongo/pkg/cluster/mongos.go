package shmongo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/util"
	mongoshardedconfig "github.com/transferia/transferia/recipe/mongo/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

type MongoS struct {
	Host        string
	Port        int
	PidFilePath string
	Config      ConfigReplicaSet
	Shards      []ShardReplicaSet
}

func (s MongoS) Fqdn() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

func (s MongoS) Close() error {
	pidInt, err := extractPidFromPidFile(s.PidFilePath)
	if err != nil {
		return xerrors.Errorf("cannot extract pid: %w", err)
	}
	return closeWithPidInt(pidInt)
}

func StartSingleMongos(
	logger log.Logger,
	binInfo EnvironmentInfo,
	configReplicaSet ConfigReplicaSet,
	shardReplicaSets []ShardReplicaSet,
	config interface{}) (MongoS, error) {

	logger.Infof("Starting mongos instance")

	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	// prepare essential configuration values and allocate machine resources
	commonSubPath := fmt.Sprintf("mongos_%d", 0)
	dbPath := path.Join(binInfo.WorkspacePath, commonSubPath, "storage")
	logPath := path.Join(binInfo.LogsPath, commonSubPath, "mongod.log")
	pidPath := path.Join(binInfo.WorkspacePath, commonSubPath, "mongos.pid")

	cfgHostList := strings.Join(yslices.Map(configReplicaSet.MongoDaemons, func(d MongoD) string {
		return d.Fqdn()
	}), ",")
	cfgHostsURL := fmt.Sprintf("%s/%s", configReplicaSet.ReplicaSet, cfgHostList)

	for _, pathToMake := range []string{dbPath, path.Dir(logPath), path.Dir(pidPath)} {
		if err := os.MkdirAll(pathToMake, 0700); err != nil {
			return MongoS{}, xerrors.Errorf("cannot make path '%s': %w", pathToMake, err)
		}
	}

	port, err := getPort(config)
	if err != nil {
		return MongoS{}, xerrors.Errorf("cannot get port for config: %w", err)
	}

	// save original user configuration for diagnostics
	originalConfigPath := path.Join(binInfo.LogsPath, commonSubPath, "original.config.yaml")
	err = mongoshardedconfig.ProduceMongodConfig(config, originalConfigPath)
	if err != nil {
		return MongoS{},
			xerrors.Errorf("cannot produce original config file for config replica set: %w", err)
	}

	// override configuration with necessary parameters
	config = mongoshardedconfig.OverridePathValue(config, mongoshardedconfig.PropMongoSConfigDB, cfgHostsURL)
	config = mongoshardedconfig.OverridePathValue(config, mongoshardedconfig.PropNetPort, port)
	config = mongoshardedconfig.OverridePathValue(config, mongoshardedconfig.PropNetBindIP, "localhost")
	config = mongoshardedconfig.OverridePathValue(config, mongoshardedconfig.PropUnixDomainSocket, false)
	config = mongoshardedconfig.OverridePathValue(config, mongoshardedconfig.PropFork, true)
	config = mongoshardedconfig.OverridePathValue(config, mongoshardedconfig.PropPidFile, pidPath)
	config = mongoshardedconfig.OverridePathValue(config, mongoshardedconfig.PropSystemLogDestination, "file")
	config = mongoshardedconfig.OverridePathValue(config, mongoshardedconfig.PropSystemLogPath, logPath)

	// save working configuration before launching mongod instance
	configPath := path.Join(binInfo.LogsPath, commonSubPath, "config.yaml")
	err = mongoshardedconfig.ProduceMongodConfig(config, configPath)
	if err != nil {
		return MongoS{},
			xerrors.Errorf("cannot produce original config file for config replica set: %w", err)
	}

	err = launchMongos(binInfo, configPath)
	if err != nil {
		return MongoS{}, xerrors.Errorf("cannot start mongod instance: %w", err)
	}
	mongos := MongoS{
		Host:        "localhost",
		Port:        port,
		PidFilePath: pidPath,
		Config:      configReplicaSet,
		Shards:      shardReplicaSets,
	}
	rollbacks.Add(func() {
		err := mongos.Close()
		if err != nil {
			logger.Error("cannot close mongos in rollback", log.Error(err))
		}
	})

	err = mongos.WithRootConnection(func(client *mongo.Client) error {
		for _, shard := range shardReplicaSets {
			err := mongos.addShardReplicaSet(client, shard)
			if err != nil {
				return xerrors.Errorf("cannot add shard to mongos: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return MongoS{}, xerrors.Errorf("root context failed: %w", err)
	}

	rollbacks.Cancel()
	return mongos, nil
}

func launchMongos(binInfo EnvironmentInfo, configPath string) error {
	mongoSExecPath := binInfo.MongoSPath()
	argv := []string{
		"--config", configPath,
	}

	cmd := exec.Command(mongoSExecPath, argv...)
	ldPreloadEnv := fmt.Sprintf("LD_PRELOAD=%s", strings.Join(binInfo.LdPreload, " "))
	cmd.Env = append(cmd.Env, ldPreloadEnv)
	co, err := cmd.CombinedOutput()
	if err != nil {
		return xerrors.Errorf("cannot start command: %w, combined output:\n%s", err, co)
	}

	return nil
}

func (s MongoS) WithRootConnection(ctxFunc func(client *mongo.Client) error) (errResult error) {
	hostSpec := s.Fqdn()
	client, err := mongo.NewClient(
		new(options.ClientOptions).SetHosts([]string{hostSpec}),
	)
	if err != nil {
		return xerrors.Errorf("unable to create client for mongodb: %w", err)
	}
	err = client.Connect(context.TODO())
	if err != nil {
		return xerrors.Errorf("unable to connect to mongodb: %w", err)
	}
	defer func() {
		err := client.Disconnect(context.TODO())
		if err != nil {
			errResult = util.NewErrs(err, errResult)
		}
	}()

	return ctxFunc(client)
}

func (s MongoS) addShardReplicaSet(client *mongo.Client, shard ShardReplicaSet) error {
	shardHostList := strings.Join(yslices.Map(shard.MongoDaemons, func(d MongoD) string {
		return d.Fqdn()
	}), ",")
	shardHostsURL := fmt.Sprintf("%s/%s", shard.ReplicaSet, shardHostList)
	sr := client.Database("admin").RunCommand(context.TODO(),
		bson.D{{Key: "addShard", Value: shardHostsURL}})
	if sr.Err() != nil {
		return sr.Err()
	}
	return nil
}
