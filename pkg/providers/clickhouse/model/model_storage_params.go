package model

import (
	"context"
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

// ch

type ChStorageParams struct {
	ConnectionParams *connectionParams
	ChClusterName    string
	BufferSize       uint64
	IOHomoFormat     ClickhouseIOFormat // one of - https://clickhouse.com/docs/en/interfaces/formats
}

func (c *ChStorageParams) IsManaged() bool {
	return c.ConnectionParams.ClusterID != ""
}

func (s *ChSource) ToStorageParams() (*ChStorageParams, error) {
	connParams, err := ConnectionParamsFromSource(s)
	if err != nil {
		return nil, xerrors.Errorf("unable to get connection params from source: %w", err)
	}

	storageParams := &ChStorageParams{
		ConnectionParams: connParams,
		ChClusterName:    s.ChClusterName,
		BufferSize:       s.BufferSize,
		IOHomoFormat:     s.IOHomoFormat,
	}

	return storageParams, nil
}

func (d *ChDestination) ToStorageParams() (*ChStorageParams, error) {
	connParams, err := ConnectionParamsFromDestination(d)
	if err != nil {
		return nil, xerrors.Errorf("unable to get connection params from source: %w", err)
	}

	storageParams := &ChStorageParams{
		ConnectionParams: connParams,
		ChClusterName:    d.ChClusterName,
		BufferSize:       20 * 1024 * 1024,
		IOHomoFormat:     "",
	}

	return storageParams, nil
}

// dataagent/ch.ConnConfig interface impl

type connConfigWrapper struct {
	p *ChStorageParams
}

func (w connConfigWrapper) RootCertPaths() []string {
	return nil
}

func (w connConfigWrapper) User() string {
	return w.p.ConnectionParams.User
}

func (w connConfigWrapper) Password() string {
	return w.p.ConnectionParams.Password
}
func (w connConfigWrapper) ResolvePassword() (string, error) {
	params := w.p.ConnectionParams
	password, err := ResolvePassword(params.ClusterID, params.User, params.Password)
	return password, err
}

func (w connConfigWrapper) Database() string {
	if w.p.ConnectionParams.Database == "*" {
		return ""
	}
	return w.p.ConnectionParams.Database
}

func (w connConfigWrapper) SSLEnabled() bool {
	return w.p.ConnectionParams.Secure
}

func (w connConfigWrapper) PemFileContent() string {
	return w.p.ConnectionParams.PemFileContent
}

func (c *ChStorageParams) ToConnParams() connConfigWrapper {
	return connConfigWrapper{p: c}
}

func (c *ChStorageParams) String() string {
	return fmt.Sprintf("%v/%v", c.ConnectionParams.Shards, c.ConnectionParams.Database)
}

func resolveHosts(clucterID, shardGroup string, shardList []ClickHouseShard, nativePort, httpPort int) ([]*clickhouse.Host, map[string][]*clickhouse.Host, error) {
	if clucterID != "" {
		return ResolveShardGroupHostsAndShards(clucterID, shardGroup, nativePort, httpPort)
	}

	hosts, shards := resolveShardsAndHosts(shardList, nativePort, httpPort)
	return hosts, shards, nil
}

func resolveShardsAndHosts(shardList []ClickHouseShard, nativePort, httpPort int) ([]*clickhouse.Host, map[string][]*clickhouse.Host) {
	var connectionHosts []*clickhouse.Host
	shards := make(map[string][]*clickhouse.Host)
	for _, shard := range shardList {
		for _, host := range shard.Hosts {
			connectionHost := &clickhouse.Host{
				Name:       host,
				NativePort: nativePort,
				HTTPPort:   httpPort,
				ShardName:  shard.Name,
			}
			connectionHosts = append(connectionHosts, connectionHost)
			shards[shard.Name] = append(shards[shard.Name], connectionHost)
		}
	}

	return connectionHosts, shards
}

func resolveConnection(connectionID string) (*clickhouse.Connection, error) {
	connCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	//DP agent token here
	conn, err := connection.Resolver().ResolveConnection(connCtx, connectionID, "ch")
	if err != nil {
		return nil, err
	}

	chConn, ok := conn.(*clickhouse.Connection)
	if !ok {
		return nil, xerrors.Errorf("Cannot cast connection %s to ClickHouse connection", connectionID)
	}
	return chConn, nil
}
