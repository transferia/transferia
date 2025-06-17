//go:build !disable_clickhouse_provider

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
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
