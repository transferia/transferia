package mongo_sharded_cluster

import "path"

const (
	EnvMongoShardedClusterHost       = "MONGO_SHARDED_CLUSTER_HOST"
	EnvMongoShardedClusterPort       = "MONGO_SHARDED_CLUSTER_PORT"
	EnvMongoShardedClusterUsername   = "MONGO_SHARDED_CLUSTER_USERNAME"
	EnvMongoShardedClusterPassword   = "MONGO_SHARDED_CLUSTER_PASSWORD"
	EnvMongoShardedClusterAuthSource = "MONGO_SHARDED_CLUSTER_AUTH_SOURCE"
)

type EnvironmentInfo struct {
	BinaryPath    string   // path to mongo server
	WorkspacePath string   // path with write access where to store cluster info and logs
	LogsPath      string   // separate path for logs
	LdPreload     []string // aux dynamic libraries to preload before launch
}

func (b EnvironmentInfo) MongoDPath() string {
	return path.Join(b.BinaryPath, "bin/mongod")
}

func (b EnvironmentInfo) MongoSPath() string {
	return path.Join(b.BinaryPath, "bin/mongos")
}

func (b EnvironmentInfo) PidFilePath() string {
	return path.Join(b.LogsPath, "mongods.pid")
}
