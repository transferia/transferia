package model

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

func TestExactlyOnceAltNamesValidation(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		aliases []model.AltName
		invalid bool
	}{
		{name: "no aliases", aliases: nil, invalid: false},
		{name: "dash collision", aliases: []model.AltName{{From: "orders-live", To: "a"}, {From: "orders_live", To: "b"}}, invalid: true},
		{name: "dot collision", aliases: []model.AltName{{From: "orders.live", To: "a"}, {From: "orders_live", To: "b"}}, invalid: true},
		{name: "two normalized aliases", aliases: []model.AltName{{From: "orders-live", To: "a"}, {From: "orders.live", To: "b"}}, invalid: true},
		{name: "same destination", aliases: []model.AltName{{From: "orders-live", To: "a"}, {From: "orders_live", To: "a"}}, invalid: false},
		{name: "same normalized destination", aliases: []model.AltName{{From: "orders-live", To: "a-b"}, {From: "orders_live", To: "a.b"}}, invalid: false},
		{name: "empty alias conflicts", aliases: []model.AltName{{From: "orders-live", To: ""}, {From: "orders_live", To: "other"}}, invalid: true},
		{name: "empty alias keeps original name", aliases: []model.AltName{{From: "orders-live", To: ""}, {From: "orders_live", To: "orders-live"}}, invalid: false},
		{name: "exact source duplicate uses last value", aliases: []model.AltName{{From: "orders-live", To: "a"}, {From: "orders-live", To: "b"}, {From: "orders_live", To: "b"}}, invalid: false},
		{name: "distinct sources", aliases: []model.AltName{{From: "orders", To: "a"}, {From: "archive", To: "b"}}, invalid: false},
	} {
		for _, exactlyOnce := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/eo=%t", scenario.name, exactlyOnce), func(t *testing.T) {
				destination := new(ChDestination)
				destination.ExactlyOnceSnapshot = exactlyOnce
				destination.AltNamesList = scenario.aliases

				err := destination.Validate()
				if exactlyOnce && scenario.invalid {
					require.ErrorContains(t, err, "ambiguous AltNames")
					for _, alias := range scenario.aliases {
						require.Contains(t, err.Error(), alias.From)
					}
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

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

func TestInsertParamsAsQueryPart(t *testing.T) {
	require.Equal(t, "", InsertParams{}.AsQueryPart())
	require.Equal(
		t,
		"SETTINGS materialized_views_ignore_errors = '1'",
		InsertParams{MaterializedViewsIgnoreErrors: true}.AsQueryPart(),
	)
	require.Equal(
		t,
		"SETTINGS materialized_views_ignore_errors = '1'",
		InsertParams{MaterializedViewsIgnoreErrors: true, UnlimitedPartitionsPerInsertBlock: true}.AsQueryPart(),
	)
}

func TestInsertParamsAsURLParams(t *testing.T) {
	require.Empty(t, InsertParams{}.AsURLParams())
	require.Empty(t, InsertParams{MaterializedViewsIgnoreErrors: true}.AsURLParams())
	require.Equal(
		t,
		url.Values{"max_partitions_per_insert_block": []string{"0"}},
		InsertParams{UnlimitedPartitionsPerInsertBlock: true}.AsURLParams(),
	)
}
