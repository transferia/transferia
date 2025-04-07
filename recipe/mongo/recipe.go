// Basic instruction for Go recipes:
// https://docs.yandex-team.ru/devtools/test/environment#create-recipe
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/library/go/test/recipe"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/recipe/mongo/pkg/binurl"
	mongoshardedcluster "github.com/transferia/transferia/recipe/mongo/pkg/cluster"
	mongoshardedconfig "github.com/transferia/transferia/recipe/mongo/pkg/config"
	tarutil "github.com/transferia/transferia/recipe/mongo/pkg/tar"
	pathutil "github.com/transferia/transferia/recipe/mongo/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

type shardedMongoRecipe struct{}

func (y *shardedMongoRecipe) isLaunchedInDistBuild() bool {
	val, ok := os.LookupEnv("YA_TEST_RUNNER")
	return ok && val != ""
}

func (y *shardedMongoRecipe) publishEnvVariables(
	config mongoshardedconfig.MongoShardedClusterConfig,
	cluster mongoshardedcluster.Cluster,
) error {
	mongos := cluster.MongoSList[0]
	// this is what you get when start recipe
	for key, value := range map[string]string{
		config.EnvPrefix + mongoshardedcluster.EnvMongoShardedClusterHost:       mongos.Host,
		config.EnvPrefix + mongoshardedcluster.EnvMongoShardedClusterPort:       fmt.Sprint(mongos.Port),
		config.EnvPrefix + mongoshardedcluster.EnvMongoShardedClusterUsername:   fmt.Sprintf(config.PostSteps.CreateAdminUser.User),
		config.EnvPrefix + mongoshardedcluster.EnvMongoShardedClusterPassword:   fmt.Sprintf(config.PostSteps.CreateAdminUser.Password),
		config.EnvPrefix + mongoshardedcluster.EnvMongoShardedClusterAuthSource: fmt.Sprintf(config.PostSteps.CreateAdminUser.AuthSource),
	} {
		recipe.SetEnv(key, value)
	}
	return nil
}

var (
	MongoVersion3 = semver.MustParse("3.0.0")
	MongoVersion4 = semver.MustParse("4.0.0")
	MongoVersion5 = semver.MustParse("5.0.0")
	MongoVersion6 = semver.MustParse("6.0.0")

	MongoVersion4Dot4 = semver.MustParse("4.4.0")
)

const (
	MongoBinaryRoot   = "sharded-mongodb/mongodb-binaries"
	SharedObjectsPath = "sharded-mongodb/so/"
	LibCurlSO440      = "libcurl.so.4.4.0"
	LibCurlSO450      = "libcurl.so.4.5.0"
	LibIdnSO          = "libidn.so.11"
	LibSslSO100       = "libssl.so.1.0.0"
	LibCryptoSO       = "libcrypto.so.1.1"
)

func (y *shardedMongoRecipe) archiveNamesToBinaryLinks() ([]binurl.BinaryLinks, error) {
	entries, err := os.ReadDir(yatest.WorkPath(MongoBinaryRoot))
	if err != nil {
		return nil, xerrors.Errorf("cannot list meta archive root: %w", err)
	}

	result := []binurl.BinaryLinks{}

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tgz") {
			link := binurl.MakeBinaryLinkByTag(strings.TrimSuffix(entry.Name(), ".tgz"))
			if link == nil {
				continue
			}
			result = append(result, *link)
		}
	}
	return result, nil
}

// selectBinaryPrefix function selects the best packet (MongoDB binary) in packet with packets
func (y *shardedMongoRecipe) selectSuitableBinaryLink(binaryLinks []binurl.BinaryLinks, version string) (binurl.BinaryLinks, error) {
	links := yslices.Filter(binaryLinks, func(binLink binurl.BinaryLinks) bool {
		return version == fmt.Sprintf("%d.%d", binLink.Version.Major, binLink.Version.Minor)
	})
	maxPatchLinks, err := binurl.TakeMaxPatchVersion(links)
	if err == nil {
		// no return error: best effort on some archive
		links = maxPatchLinks
	}
	maxDistrLinks, err := binurl.TakeMaxDistrVersion(links)
	if err == nil {
		// no return error: best effort on some archive
		links = maxDistrLinks
	}

	if len(links) == 0 {
		return binurl.BinaryLinks{}, xerrors.Errorf("couldn't find suitable MongoDB binary for version '%s'", version)
	}

	return links[0], nil
}

// produceMongoDBBinaryPath looks up existing archives, finds the
// most suitable and extracts it, then returns MongoDB path for
// further use. Specify version parameter to narrow down desired archives
func (y *shardedMongoRecipe) produceMongoDBBinaryPath(version string) (string, error) {
	links, err := y.archiveNamesToBinaryLinks()
	if err != nil {
		return "", xerrors.Errorf("cannot list MongoDB binaries to extract platform and version info: %w", err)
	}
	binLink, err := y.selectSuitableBinaryLink(links, version)
	if err != nil {
		return "", xerrors.Errorf("cannot find suitable MongoDB binary archive: %w", err)
	}
	destination := path.Join(MongoBinaryRoot, binLink.Tag)
	source := fmt.Sprintf("%s.tgz", destination)
	err = tarutil.UnpackArchive(yatest.WorkPath(source), yatest.WorkPath(destination))
	if err != nil {
		return "", xerrors.Errorf("unable to unpack archive '%s' to '%s': %w", source, destination, err)
	}

	dirEntries, err := os.ReadDir(destination)
	if err != nil {
		return "", xerrors.Errorf("cannot read destination archive directory: %w", err)
	}
	if len(dirEntries) > 1 {
		return "", xerrors.Errorf("archive content is ambiguous: %s",
			yslices.Map(dirEntries, func(entry os.DirEntry) string { return entry.Name() }),
		)
	}
	return path.Join(destination, dirEntries[0].Name()), nil
}

func (y *shardedMongoRecipe) startCluster(logger log.Logger, cfgID int, configFilePath string) (
	mongoshardedcluster.Cluster,
	mongoshardedconfig.MongoShardedClusterConfig,
	mongoshardedcluster.EnvironmentInfo,
	error,
) {
	var dummyLgr mongoshardedcluster.Cluster
	var dummyCfg mongoshardedconfig.MongoShardedClusterConfig
	var dummyEnvInfo mongoshardedcluster.EnvironmentInfo
	absoluteConfigFilePath := yatest.SourcePath(configFilePath)
	clusterConfig, err := mongoshardedconfig.GetConfigFromYaml(absoluteConfigFilePath)
	if err != nil {
		return dummyLgr, dummyCfg, dummyEnvInfo, xerrors.Errorf("Cannot parse config: %w", err)
	}

	mongoBinaryPath, err := y.produceMongoDBBinaryPath(clusterConfig.MongoDBVersion)
	if err != nil {
		return dummyLgr, dummyCfg, dummyEnvInfo, xerrors.Errorf("failed unpacking any MongoDB binary from meta archive: %w", err)
	}

	logger.Info("MongoDB binary selected", log.String("name", mongoBinaryPath))

	mongoVersion, err := semver.ParseTolerant(clusterConfig.MongoDBVersion)
	if err != nil {
		return dummyLgr, dummyCfg, dummyEnvInfo, xerrors.Errorf("cannot parse config version: %w", err)
	}

	libCurl440Path := yatest.WorkPath(path.Join(SharedObjectsPath, LibCurlSO440))
	libCurl450Path := yatest.WorkPath(path.Join(SharedObjectsPath, LibCurlSO450))
	LibIdnSOPath := yatest.WorkPath(path.Join(SharedObjectsPath, LibIdnSO))
	LibSslSO110Path := yatest.WorkPath(path.Join(SharedObjectsPath, LibSslSO100))
	LibCryptoSOPath := yatest.WorkPath(path.Join(SharedObjectsPath, LibCryptoSO))

	// specifying curl dependency
	ldPreload := []string{}
	if mongoVersion.GTE(MongoVersion4Dot4) {
		ldPreload = append(ldPreload, libCurl450Path, LibIdnSOPath, LibSslSO110Path, LibCryptoSOPath)
	} else {
		if y.isLaunchedInDistBuild() {
			return dummyLgr, dummyCfg, dummyEnvInfo, xerrors.Errorf("MongoDB versions lower than 4.4 is not supported in recipes because distbuild uses Ubuntu2004 (see full list of binaries: https://www.mongodb.com/download-center/community/releases/archive)")
		}
	}
	if mongoVersion.GTE(MongoVersion3) && mongoVersion.LT(MongoVersion4Dot4) {
		ldPreload = append(ldPreload, libCurl440Path)
	}

	envInfo := mongoshardedcluster.EnvironmentInfo{
		BinaryPath:    yatest.WorkPath(mongoBinaryPath),
		WorkspacePath: pathutil.MakeWorkPath(fmt.Sprintf("workspace_%d", cfgID)),
		LogsPath:      pathutil.MakeOutputPath(fmt.Sprintf("logs_%d", cfgID)),
		LdPreload:     ldPreload,
	}

	cluster, err := mongoshardedcluster.StartCluster(logger, envInfo, clusterConfig)
	if err != nil {
		return dummyLgr, dummyCfg, dummyEnvInfo, xerrors.Errorf("cannot start sharded cluster: %w", err)
	}

	allPids, _ := cluster.GetAllPids()
	logger.Info("cluster has been successfully started!",
		log.Ints("ports_mongos", yslices.Map(cluster.MongoSList, func(s mongoshardedcluster.MongoS) int {
			return s.Port
		})),
		log.Ints("all_pids", allPids),
	)

	return cluster, clusterConfig, envInfo, nil
}

func (y *shardedMongoRecipe) Start() error {
	logger, err := zap.New(zap.CLIConfig(log.InfoLevel))
	if err != nil {
		return xerrors.Errorf("cannot create logger: %w", err)
	}

	logger.Info("Starting MongoDB sharded cluster recipe...")

	configFilePaths, ok := os.LookupEnv("MONGO_SHARDED_CLUSTER_CONFIG")
	if !ok {
		return xerrors.Errorf("config file is not specified, you should specify it via env variable MONGO_SHARDED_CLUSTER_CONFIG " +
			"in 'ya.make' file like this: ENV(MONGO_SHARDED_CLUSTER_CONFIG=\"cluster_config.yaml\"." +
			"Note, that you should also add directive DATA(arcadia/<path-to-your>/cluster_config.yaml)")
	}
	logger.Infof("Using config files: %s", configFilePaths)

	configFilePathsArr := strings.Split(configFilePaths, ":")
	if len(configFilePathsArr) == 0 {
		return xerrors.Errorf("zero config files specified!")
	}

	var rollbacks util.Rollbacks
	defer rollbacks.Do()
	envInfos := []mongoshardedcluster.EnvironmentInfo{}
	for cfgID, configFilePath := range configFilePathsArr {
		logger.Info("Process config",
			log.Int("config_index", cfgID),
			log.String("config_path", configFilePath),
		)
		cluster, clusterConfig, envInfo, err := y.startCluster(logger, cfgID, configFilePath)
		if err != nil {
			return xerrors.Errorf("cannot start cluster #%d: %w", cfgID+1, err)
		}

		rollbacks.Add(func() {
			err = mongoshardedcluster.StopCluster(logger, envInfo)
			if err != nil {
				logger.Error("Cannot stop mongo sharded cluster", log.Error(err))
			}
		})

		err = y.publishEnvVariables(clusterConfig, cluster)
		if err != nil {
			return xerrors.Errorf("Unable to publish env variables: %w", err)
		}

		envInfos = append(envInfos, envInfo)
	}

	marshalledEnvInfo, err := json.Marshal(envInfos)
	if err != nil {
		return xerrors.Errorf("cannot marshal recipe environment info: %w", err)
	}

	envInfoPath := yatest.WorkPath("env.info.json")
	err = os.WriteFile(envInfoPath, marshalledEnvInfo, 0755)
	if err != nil {
		return xerrors.Errorf("cannot write recipe environment info: %w", err)
	}

	rollbacks.Cancel()
	return nil
}

func (y *shardedMongoRecipe) Stop() error {
	logger, err := zap.New(zap.CLIConfig(log.InfoLevel))
	if err != nil {
		return xerrors.Errorf("cannot create logger: %w", err)
	}

	logger.Infof("Stopping MongoDB sharded cluster recipe...")

	envInfoPath := yatest.WorkPath("env.info.json")
	marshalledEnvInfo, err := os.ReadFile(envInfoPath)
	if err != nil {
		return xerrors.Errorf("cannot read recipe environment info from file: %w", err)
	}

	var envInfos []mongoshardedcluster.EnvironmentInfo
	err = json.Unmarshal(marshalledEnvInfo, &envInfos)
	if err != nil {
		return xerrors.Errorf("cannot unmarshal recipe environment info from file: %w", err)
	}

	for i, envInfo := range envInfos {
		err = mongoshardedcluster.StopCluster(logger, envInfo)
		if err != nil {
			logger.Error("Cannot stop sharded cluster", log.Int("index", i), log.Error(err))
		}
	}

	return nil
}

func main() {
	recipe.Run(&shardedMongoRecipe{})
}
