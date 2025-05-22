package model

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

type connectionParams struct {
	ClusterID      string
	User           string
	Password       string
	Database       string
	Secure         bool
	PemFileContent string
	Hosts          []*clickhouse.Host
	Shards         map[string][]*clickhouse.Host
}

func (c connectionParams) SetShards(shards map[string][]*clickhouse.Host) {
	for shardName, hosts := range shards {
		c.Shards[shardName] = append(c.Shards[shardName], hosts...)
	}
}

func ConnectionParamsFromSource(chSource *ChSource) (*connectionParams, error) {
	var connParams *connectionParams
	if chSource.ConnectionID != "" {
		params, err := ConnectionParamsByConnectionID(chSource.ConnectionID, chSource.ChClusterName, chSource.NativePort, chSource.HTTPPort)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve connection params by connection ID: %w", err)
		}
		connParams = params
	} else {
		connectionHosts, shards, err := resolveHosts(chSource.MdbClusterID, chSource.ChClusterName, chSource.ShardsList, chSource.NativePort, chSource.HTTPPort)
		if err != nil {
			return nil, err
		}
		secure := chSource.SSLEnabled || chSource.MdbClusterID != ""
		connParams = &connectionParams{
			ClusterID:      chSource.MdbClusterID,
			User:           chSource.User,
			Password:       string(chSource.Password),
			Secure:         secure,
			PemFileContent: chSource.PemFileContent,
			Database:       chSource.Database,
			Hosts:          connectionHosts,
			Shards:         shards,
		}
	}

	return connParams, nil
}

func ConnectionParamsFromDestination(chDestination *ChDestination) (*connectionParams, error) {
	var connParams *connectionParams
	if chDestination.ConnectionID != "" {
		params, err := ConnectionParamsByConnectionID(chDestination.ConnectionID, chDestination.ChClusterName, chDestination.NativePort, chDestination.HTTPPort)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve connection params by connection ID: %w", err)
		}
		connParams = params
	} else {
		connectionHosts, shards, err := resolveHosts(chDestination.MdbClusterID, chDestination.ChClusterName, chDestination.ShardsList, chDestination.NativePort, chDestination.HTTPPort)
		if err != nil {
			return nil, err
		}
		secure := chDestination.SSLEnabled || chDestination.MdbClusterID != ""
		connParams = &connectionParams{
			ClusterID:      chDestination.MdbClusterID,
			User:           chDestination.User,
			Password:       string(chDestination.Password),
			Secure:         secure,
			PemFileContent: chDestination.PemFileContent,
			Database:       chDestination.Database,
			Hosts:          connectionHosts,
			Shards:         shards,
		}
	}

	return connParams, nil
}

func ConnectionParamsByConnectionID(connectionID string, shardGroup string, nativePort int, httpPort int) (*connectionParams, error) {
	if connectionID == "" {
		return nil, xerrors.Errorf("connection ID not filled")
	}

	conn, err := resolveConnection(connectionID)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve connection: %w", err)
	}

	result := &connectionParams{
		ClusterID:      conn.ClusterID,
		User:           conn.User,
		Password:       string(conn.Password),
		Database:       conn.Database,
		Secure:         conn.HasTLS,
		PemFileContent: conn.CACertificates,
		Hosts:          conn.Hosts,
		Shards:         make(map[string][]*clickhouse.Host),
	}

	if conn.ClusterID == "" {
		result.Shards = resolveShardsFromHosts(result.Hosts)
	} else {
		hosts, shards, err := ResolveShardGroupHostsAndShards(conn.ClusterID, shardGroup, nativePort, httpPort)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve shard group hosts and shards: %w", err)
		}
		result.Hosts = hosts
		result.Shards = shards
	}

	return result, nil
}

func resolveShardsFromHosts(hosts []*clickhouse.Host) map[string][]*clickhouse.Host {
	shards := make(map[string][]*clickhouse.Host)
	for _, host := range hosts {
		shards[host.ShardName] = append(shards[host.ShardName], host)
	}

	return shards
}
