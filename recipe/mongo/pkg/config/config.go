package shmongocfg

import (
	_ "embed"
	"os"
	"regexp"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"gopkg.in/yaml.v2"
)

const (
	MongoDefaultVersion = "4.4"
)

var (
	MongoMajorVersionRegexp = regexp.MustCompile(`\d+.\d+`)

	PropClusterRoleName      = []string{"sharding", "clusterRole"}
	PropMongoSConfigDB       = []string{"sharding", "configDB"}
	PropReplicationSetName   = []string{"replication", "replSetName"}
	PropStorageDBPath        = []string{"storage", "dbPath"}
	PropNetPort              = []string{"net", "port"}
	PropNetBindIP            = []string{"net", "bindIp"}
	PropUnixDomainSocket     = []string{"net", "unixDomainSocket", "enabled"}
	PropFork                 = []string{"processManagement", "fork"}
	PropPidFile              = []string{"processManagement", "pidFilePath"}
	PropSystemLogDestination = []string{"systemLog", "destination"}
	PropSystemLogPath        = []string{"systemLog", "path"}
)

// MongoShardedClusterConfig repsresents config for whole sharded cluster
type MongoShardedClusterConfig struct {
	MongoDBVersion   string           `yaml:"version"`
	EnvPrefix        string           `yaml:"envPrefix"`
	PostSteps        PostStepsConfig  `yaml:"postSteps"`
	ConfigReplicaSet ReplicaSetConfig `yaml:"configReplicaSet"`
	ShardReplicaSet  ShardsConfig     `yaml:"shards"`
	// TODO(@kry127) support multiple mongos instances + add NoOverride support
	Mongos interface{} `yaml:"mongos"`
}

func (c *MongoShardedClusterConfig) WithDefaults() {
	if c.MongoDBVersion == "" {
		c.MongoDBVersion = MongoDefaultVersion
	}
}

func (c *MongoShardedClusterConfig) Validate() error {
	if !MongoMajorVersionRegexp.MatchString(c.MongoDBVersion) {
		return xerrors.Errorf("version '%s' does not comply strict format 'x.x', e.g. '4.4'", c.MongoDBVersion)
	}
	return nil
}

type PostStepsConfig struct {
	CreateAdminUser CreateAdminUserConfig `yaml:"createAdminUser"`
}

type CreateAdminUserConfig struct {
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	AuthSource string `yaml:"authSource"`
}

// ReplicaSetConfig represents config for sharded cluster
// semantics:
//   - Configs is configuration of each mongod instance in cluster.
//   - If Amount and Config are specified, the configuration Config appended Amount times
//     to the configuration Configs
//   - if NoOverride has been explicitly set to true, then no modification
//     of configuration will be performed. This is useful for legacy scenarios where some flags
//     may be missing.
type ReplicaSetConfig struct {
	NoOverride bool          `yaml:"noOverride"`
	Amount     uint          `yaml:"amount"`
	Config     interface{}   `yaml:"config"`
	Configs    []interface{} `yaml:"configs"`
}

func (r *ReplicaSetConfig) MongodConfigCount() uint {
	return r.Amount + uint(len(r.Configs))
}

func (r *ReplicaSetConfig) GetMongodConfig(i uint) (interface{}, error) {
	if i < r.Amount {
		return r.Config, nil
	}
	if i-r.Amount < uint(len(r.Configs)) {
		return r.Configs[i-r.Amount], nil
	}
	return nil, xerrors.Errorf("Invalid index of RS config: %d", i)
}

type ShardsConfig struct {
	Amount  uint               `yaml:"amount"`
	Config  ReplicaSetConfig   `yaml:"shardReplicaSet"`
	Configs []ReplicaSetConfig `yaml:"shardReplicaSets"`
}

func (s *ShardsConfig) ReplicaSetCount() uint {
	return s.Amount + uint(len(s.Configs))
}

func (s *ShardsConfig) GetReplicaSetConfig(i uint) (ReplicaSetConfig, error) {
	if i < s.Amount {
		return s.Config, nil
	}
	if i-s.Amount < uint(len(s.Configs)) {
		return s.Configs[i-s.Amount], nil
	}
	return ReplicaSetConfig{}, xerrors.Errorf("Invalid index of RS config: %d", i)
}

func GetConfigFromYaml(configPath string) (MongoShardedClusterConfig, error) {
	configYamlFile, err := os.ReadFile(configPath)
	if err != nil {
		return MongoShardedClusterConfig{}, xerrors.Errorf("cannot read configuration file: %w", err)
	}

	var config MongoShardedClusterConfig

	err = yaml.Unmarshal(configYamlFile, &config)
	if err != nil {
		return MongoShardedClusterConfig{}, xerrors.Errorf("cannot unmarshal configuration: %w", err)
	}

	config.WithDefaults()
	if err := config.Validate(); err != nil {
		return MongoShardedClusterConfig{}, xerrors.Errorf("validation error: %w", err)
	}
	return config, nil
}

func ProduceMongodConfig(object interface{}, path string) error {
	bytes, err := yaml.Marshal(object)
	if err != nil {
		return xerrors.Errorf("cannot marshal configuration object: %w", err)
	}
	err = os.WriteFile(path, bytes, 0644)
	if err != nil {
		return xerrors.Errorf("cannot write configuration object as yaml: %w", err)
	}
	return nil
}

func OverridePathValue(original interface{}, path []string, value interface{}) interface{} {
	if len(path) == 0 {
		return value
	}
	originalMap, isOriginalMap := original.(map[string]interface{})
	if !isOriginalMap {
		return OverridePathValue(map[string]interface{}{}, path, value)
	}
	result := make(map[string]interface{}, len(originalMap))
	for k, v := range originalMap {
		result[k] = v
	}

	key := path[0]
	var mergeWith interface{} = map[string]interface{}{}
	oldValue, hasOldValue := result[key]
	if hasOldValue {
		mergeWith = oldValue
	}
	result[key] = OverridePathValue(mergeWith, path[1:], value)

	return result
}

func GetKey(original interface{}, path []string) (interface{}, bool) {
	if len(path) == 0 {
		return original, true
	}
	originalMap, isOriginalMap := original.(map[string]interface{})
	if !isOriginalMap {
		return nil, false
	}

	key := path[0]
	value, hasValue := originalMap[key]
	if !hasValue {
		return nil, false
	}
	return GetKey(value, path[1:])
}
