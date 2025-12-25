package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

func TestMakeChildShardParams(t *testing.T) {
	chDestinationWrapper := new(ChDestinationWrapper)
	chDestinationWrapper.Model = new(ChDestination)
	chDestinationWrapper.connectionParams = *new(connectionParams)
	shard1Hosts := make([]*clickhouse.Host, 2)
	shard1Hosts[0] = &clickhouse.Host{
		Name:       "host1",
		ShardName:  "shard1",
		HTTPPort:   9000,
		NativePort: 9001,
	}
	shard1Hosts[1] = &clickhouse.Host{
		Name:       "host2",
		ShardName:  "shard1",
		HTTPPort:   9000,
		NativePort: 9001,
	}

	shard2Hosts := make([]*clickhouse.Host, 1)
	shard2Hosts[0] = &clickhouse.Host{
		Name:       "host3",
		ShardName:  "shard2",
		HTTPPort:   9000,
		NativePort: 9001,
	}

	chDestinationWrapper.connectionParams.Hosts = []*clickhouse.Host{
		shard1Hosts[0], shard1Hosts[1], shard2Hosts[0],
	}

	childServerParams := chDestinationWrapper.MakeChildShardParams(shard1Hosts)
	altHosts := childServerParams.AltHosts()
	require.ElementsMatch(t, altHosts, shard1Hosts)

	childServerParams = chDestinationWrapper.MakeChildShardParams(shard2Hosts)
	altHosts = childServerParams.AltHosts()
	require.ElementsMatch(t, altHosts, shard2Hosts)
}

func TestDestinationGettersShouldBeReplacedByConnectionParams(t *testing.T) {
	chDestination := new(ChDestination)
	chDestination.ConnectionID = "chDestination.ConnectionID"
	chDestination.Database = "chDestination.Database"

	connectionResolver := connection.NewStubConnectionResolver()

	connmanConnection := &clickhouse.Connection{
		Hosts: []*clickhouse.Host{
			{Name: "clickhouse.Connection.host1", ShardName: "clickhouse.Connection.shard1", HTTPPort: 9000, NativePort: 9001},
			{Name: "clickhouse.Connection.host2", ShardName: "clickhouse.Connection.shard1", HTTPPort: 9000, NativePort: 9001},
			{Name: "clickhouse.Connection.host3", ShardName: "clickhouse.Connection.shard2", HTTPPort: 9000, NativePort: 9001},
		},
		User:           "clickhouse.Connection.User",
		Password:       model.SecretString("clickhouse.Connection.Password"),
		Database:       "clickhouse.Connection.Database",
		HasTLS:         true,
		CACertificates: "clickhouse.Connection.CACertificates",
		ClusterID:      "clickhouse.Connection.ClusterID",
		DatabaseNames:  []string{"clickhouse.Connection.DatabaseNames"},
		ShardGroups:    map[string][]string{"clickhouse.Connection.ShardGroups": {"clickhouse.Connection.shard1", "clickhouse.Connection.shard2"}},
	}

	err := connectionResolver.Add(chDestination.ConnectionID, connmanConnection)
	require.NoError(t, err)
	connection.Init(connectionResolver)

	chDestinationWrapper, err := chDestination.ToSinkParams(new(model.Transfer))
	require.NoError(t, err)

	require.Equal(t, chDestinationWrapper.MdbClusterID(), connmanConnection.ClusterID)
	require.Equal(t, chDestinationWrapper.User(), connmanConnection.User)
	require.Equal(t, chDestinationWrapper.Password(), string(connmanConnection.Password))
	require.Equal(t, chDestinationWrapper.Database(), chDestination.Database)
	require.Equal(t, chDestinationWrapper.SSLEnabled(), connmanConnection.HasTLS)
	require.Equal(t, chDestinationWrapper.PemFileContent(), connmanConnection.CACertificates)
	require.Equal(t, chDestinationWrapper.AltHosts(), connmanConnection.Hosts)
}
