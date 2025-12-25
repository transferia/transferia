package shmongo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	mongoshardedconfig "github.com/transferia/transferia/recipe/mongo/pkg/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.ytsaurus.tech/library/go/core/log"
)

type MongoD struct {
	Host        string
	Port        int
	PidFilePath string
}

func (d MongoD) Fqdn() string {
	return fmt.Sprintf("%s:%d", d.Host, d.Port)
}

type MongoDMode string

var (
	MongoDConfigSvr MongoDMode = "configsvr"
	MongoDShardSvr  MongoDMode = "shardsvr"
)

func finishProcess(process *os.Process) error {
	err := process.Signal(syscall.SIGTERM)
	if err != nil {
		return xerrors.Errorf("error sending SIGTERM signal to process: %w", err)
	}
	errChan := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() {
		_, err := process.Wait()
		errChan <- err
	}()
	select {
	case err := <-errChan:
		if err != nil {
			return xerrors.Errorf("error while waiting for process pid=%d cleanup: %w", process.Pid, err)
		}
		return nil
	case <-ctx.Done():
		err := process.Release()
		if err != nil {
			return xerrors.Errorf("unable to release resources: %w", err)
		}
		return process.Kill()
	}
}

func (d MongoD) Close() error {
	pidInt, err := extractPidFromPidFile(d.PidFilePath)
	if err != nil {
		return xerrors.Errorf("cannot extract pid: %w", err)
	}
	return closeWithPidInt(pidInt)
}

func extractPidFromString(pidAsString string) (int, error) {
	trimmedPidAsString := strings.TrimSpace(pidAsString)
	pidInt, err := strconv.Atoi(trimmedPidAsString)
	if err != nil {
		return 0, xerrors.Errorf("wrong pid format: %w", err)
	}
	return pidInt, nil
}

func extractPidFromPidFile(pidFilePath string) (int, error) {
	pidFileContent, err := os.ReadFile(pidFilePath)
	if err != nil {
		return 0, xerrors.Errorf("cannot read pid: %w", err)
	}
	return extractPidFromString(string(pidFileContent))
}

func closeWithPidInt(pidInt int) error {
	proc, err := os.FindProcess(pidInt)
	if err != nil {
		return xerrors.Errorf("cannot find process: %w", err)
	}
	return finishProcess(proc)
}

func InferReplicaSet(config mongoshardedconfig.ReplicaSetConfig, defaultValue string) (string, error) {
	found := false

	var result string
	propertyPath := []string{"replication", "replSetName"}
	for i := uint(0); i < config.MongodConfigCount(); i++ {
		cfg, err := config.GetMongodConfig(i)
		if err != nil {
			return "", xerrors.Errorf("cannot get config %d: %w", i, err)
		}
		rs, ok := mongoshardedconfig.GetKey(cfg, propertyPath)
		if !ok {
			continue
		}
		rss, ok := rs.(string)
		if !ok {
			return "", xerrors.Errorf("property '%s' should have type string", strings.Join(propertyPath, "."))
		}
		if found && result != rss {
			return "", xerrors.Errorf(
				"config replica set conflict -- found two replica set names: %s and %s -- you should specify only one custom replica set",
				result, rs)
		}
		result = rss
		found = true
	}
	if !found {
		// return standard name for config replica set
		return defaultValue, nil
	}
	return result, nil
}

func getPort(cfg interface{}) (int, error) {
	var port int
	userPort, hasUserPort := mongoshardedconfig.GetKey(cfg, mongoshardedconfig.PropNetPort)
	userPortAsInt, isPint := userPort.(int)

	if hasUserPort && isPint && userPortAsInt != 0 {
		port = userPortAsInt
	} else {
		freePort, err := util.GetFreePort()
		if err != nil {
			return 0, xerrors.Errorf("cannot get next free port: %w", err)
		}
		port = freePort
	}
	return port, nil
}

func StartReplicaSet(
	logger log.Logger,
	binInfo EnvironmentInfo,
	config mongoshardedconfig.ReplicaSetConfig,
	replicaSet string,
	mode MongoDMode,
) ([]MongoD, error) {
	logger.Infof("Starting shard with replica set '%s' with %d mongod instances", replicaSet, config.MongodConfigCount())

	if config.MongodConfigCount() == 0 {
		return nil, xerrors.Errorf("try to launch config replica set with zero mongod instances specified")
	}
	result := []MongoD{}
	var rollbacks util.Rollbacks
	defer rollbacks.Do()
	for i := uint(0); i < config.MongodConfigCount(); i++ {
		// prepare essential configuration values and allocate machine resources
		commonSubPath := fmt.Sprintf("%s_%s_mongod_%d", mode, replicaSet, i)
		dbPath := path.Join(binInfo.WorkspacePath, commonSubPath, "storage")
		logPath := path.Join(binInfo.LogsPath, commonSubPath, "mongod.log")
		pidPath := path.Join(binInfo.WorkspacePath, commonSubPath, "mongod.pid")

		for _, pathToMake := range []string{dbPath, path.Dir(logPath), path.Dir(pidPath)} {
			if err := os.MkdirAll(pathToMake, 0700); err != nil {
				return nil, xerrors.Errorf("cannot make path '%s': %w", pathToMake, err)
			}
		}

		// get configuration
		cfg, err := config.GetMongodConfig(i)
		if err != nil {
			return nil, xerrors.Errorf("cannot get config #%d: %w", i, err)
		}

		port, err := getPort(cfg)
		if err != nil {
			return nil, xerrors.Errorf("cannot get port for config: %w", err)
		}

		// save original user configuration for diagnostics
		originalConfigPath := path.Join(binInfo.LogsPath, commonSubPath, "original.config.yaml")
		err = mongoshardedconfig.ProduceMongodConfig(cfg, originalConfigPath)
		if err != nil {
			return nil,
				xerrors.Errorf("cannot produce original config file for config replica set: %w", err)
		}

		// override configuration with necessary parameters
		if config.NoOverride {
			logger.Warn("Skip override", log.String("replica_set", replicaSet))
		} else {
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropClusterRoleName, mode)
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropReplicationSetName, replicaSet)
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropStorageDBPath, dbPath)
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropNetPort, port)
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropNetBindIP, "localhost")
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropUnixDomainSocket, false)
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropFork, true)
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropPidFile, pidPath)
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropSystemLogDestination, "file")
			cfg = mongoshardedconfig.OverridePathValue(cfg, mongoshardedconfig.PropSystemLogPath, logPath)
		}

		// save working configuration before launching mongod instance
		configPath := path.Join(binInfo.LogsPath, commonSubPath, "config.yaml")
		err = mongoshardedconfig.ProduceMongodConfig(cfg, configPath)
		if err != nil {
			return nil,
				xerrors.Errorf("cannot produce original config file for config replica set: %w", err)
		}

		err = startMongoD(binInfo, configPath)
		if err != nil {
			return nil, xerrors.Errorf("cannot start mongod instance: %w", err)
		}
		mongod := MongoD{
			Host:        "localhost",
			Port:        port,
			PidFilePath: pidPath,
		}
		result = append(result, mongod)
		rollbacks.Add(func() {
			err := mongod.Close()
			if err != nil {
				logger.Error("Cannot close mongod instance", log.Any("params", cfg),
					log.Any("mongod_description", mongod))
			}
		})
	}

	rollbacks.Cancel()
	return result, nil
}

// startMongoD -- starts MongoD instance identified with subpath and configuration
// may override some settings that user set in config file
func startMongoD(binInfo EnvironmentInfo, configPath string) error {
	mongoDExecPath := binInfo.MongoDPath()
	argv := []string{
		"--config", configPath,
	}

	cmd := exec.Command(mongoDExecPath, argv...)
	ldPreloadEnv := fmt.Sprintf("LD_PRELOAD=%s", strings.Join(binInfo.LdPreload, " "))
	cmd.Env = append(cmd.Env, ldPreloadEnv)
	co, err := cmd.CombinedOutput()
	if err != nil {
		return xerrors.Errorf("cannot start command: %w, combined output:\n%s", err, co)
	}
	return nil
}

func (d MongoD) WithRootConnection(ctxFunc func(client *mongo.Client) error) (errResult error) {
	hostSpec := d.Fqdn()
	client, err := mongo.NewClient(
		new(options.ClientOptions).SetHosts([]string{hostSpec}).
			SetDirect(true),
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

	err = backoff.Retry(func() error {
		return client.Ping(context.TODO(), nil)
	}, backoff.NewExponentialBackOff())
	if err != nil {
		return xerrors.Errorf("unable to ping mongod instance: %w", err)
	}

	return ctxFunc(client)
}
