package main

import (
	"os"
	"time"

	mongoshardedcluster "github.com/transferria/transferria/recipe/mongo/pkg/cluster"
	mongoshardedconfig "github.com/transferria/transferria/recipe/mongo/pkg/config"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

func main() {
	launchCluster()
}

func launchCluster() {
	lgr, err := zap.New(zap.CLIConfig(log.InfoLevel))
	if err != nil {
		panic(err)
	}

	// Specify path to the .yaml config file that describes your cluster
	yamlConfig := "/Users/kry127/arcadia/transfer_manager/go/recipe/mongo/config/sample/auth.yaml"

	// Then parse config into our internal representation
	clusterConfig, err := mongoshardedconfig.GetConfigFromYaml(yamlConfig)
	if err != nil {
		lgr.Error("unable to parse .yaml config file", log.Error(err),
			log.String("path", yamlConfig))
		os.Exit(1)
	}

	// Provide the code with environment information:
	envInfo := mongoshardedcluster.EnvironmentInfo{
		// Download binary from official MongoDB site:
		// https://www.mongodb.com/download-center/community/releases/archive
		// Select 'Archive' option to download, unpack archieve to the folder and
		// here you should specify a path to that folder
		BinaryPath: "/Users/kry127/Desktop/mongodb-binaries/mongodb-macos-x86_64-5.0.12",
		// Define path where to persist database data and other additional info
		WorkspacePath: "/Users/kry127/Desktop/mongodb-binaries/workspace",
		// Define path where to put logs and  cluster configuration files
		LogsPath: "/Users/kry127/Desktop/mongodb-binaries/logs",
		// Advanced setting: put here dynamic libraries to use LD_PRELOAD hack
		// e.g. may be useful to inject libcurl_3 dependency without installation
		// of the library to the target OS
		LdPreload: []string{},
	}

	// Then, you are ready to start your MongoDB sharded cluster from Go code:
	cluster, err := mongoshardedcluster.StartCluster(lgr, envInfo, clusterConfig)
	if err != nil {
		// if error occured, all allocated resources in the intermediate stages will be freed
		// until executing following line
		lgr.Error("unable to start sharded cluster", log.Error(err),
			log.Any("env_info", envInfo))
		os.Exit(2)
	}
	// don't forget to close your cluster
	defer func() {
		err = mongoshardedcluster.StopCluster(lgr, envInfo)
		if err != nil {
			lgr.Error("unable to stop sharded cluster", log.Error(err),
				log.Any("env_info", envInfo))
		}
	}()

	lgr.Infof("Cluster is ready! Connect to %s with credentials username=%s, auth_source=%s",
		cluster.MongoSList[0].Fqdn(),
		clusterConfig.PostSteps.CreateAdminUser.User,
		clusterConfig.PostSteps.CreateAdminUser.AuthSource,
	)

	// here you can perform certain work with cluster
	time.Sleep(10 * time.Second)
}
