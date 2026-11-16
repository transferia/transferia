package dbaas

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var NotSupported = xerrors.NewSentinel("resolver not supported")

type ResolverFactory interface {
	HostResolver(ctx context.Context, typ ProviderType, clusterID string) (HostResolver, error)
	PasswordResolver(typ ProviderType, clusterID string) (PasswordResolver, error)
	ShardResolver(typ ProviderType, clusterID string) (ShardResolver, error)
	ShardGroupHostsResolver(ctx context.Context, typ ProviderType, clusterID string) (ShardGroupHostsResolver, error)
}

type HostResolver interface {
	ResolveHosts(ctx context.Context) ([]ClusterHost, error)
}

type PasswordResolver interface {
	ResolvePassword() (string, error)
}

type ShardResolver interface {
	Sharded() (bool, error)
}

type ShardGroupHostsResolver interface {
	// Empty shardGroup resolves the whole cluster. A nonempty requiredShards
	// limits health validation to the original shards used by the transfer.
	ResolveHostsForShards(ctx context.Context, shardGroup string, requiredShards []string) ([]ClusterHost, error)
}

type ClusterHost struct {
	Name        string       `json:"name"`
	ClusterID   string       `json:"clusterId"`
	ShardName   string       `json:"shardName"`
	Type        InstanceType `json:"type"`
	Role        Role         `json:"role"`
	Health      Health       `json:"health"`
	ReplicaType ReplicaType  `json:"replicaType"`
}

type ReplicaType string

const (
	ReplicaTypeSync    = ReplicaType("SYNC")
	ReplicaTypeAsync   = ReplicaType("ASYNC")
	ReplicaTypeUnknown = ReplicaType("UNKNOWN")
)

type ProviderType string

var (
	ProviderTypeMysql         = ProviderType("managed-mysql")
	ProviderTypeKafka         = ProviderType("managed-kafka")
	ProviderTypeMetastore     = ProviderType("managed-metastore")
	ProviderTypePostgresql    = ProviderType("managed-postgresql")
	ProviderTypeMongodb       = ProviderType("managed-mongodb")
	ProviderTypeClickhouse    = ProviderType("managed-clickhouse")
	ProviderTypeGreenplum     = ProviderType("managed-greenplum")
	ProviderTypeElasticSearch = ProviderType("managed-elasticsearch")
	ProviderTypeOpenSearch    = ProviderType("managed-opensearch")
)

type InstanceType string

const (
	InstanceTypeUnspecified = InstanceType("")
	InstanceTypeClickhouse  = InstanceType("CLICKHOUSE")
	InstanceTypeMongod      = InstanceType("MONGOD")
	InstanceTypeMongos      = InstanceType("MONGOS")
	InstanceTypeMongoinfra  = InstanceType("MONGOINFRA")
	InstanceTypeMongocfg    = InstanceType("MONGOCFG")
)
