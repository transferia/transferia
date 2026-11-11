package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

func TestSourceGettersShouldBeReplacedByConnectionParams(t *testing.T) {
	chSource := new(ChSource)
	chSource.ConnectionID = "chSource.ConnectionID"
	chSource.Database = "chSource.Database"

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

	err := connectionResolver.Add(chSource.ConnectionID, connmanConnection)
	require.NoError(t, err)
	connection.Init(connectionResolver)

	chSourceWrapper, err := chSource.ToSinkParams()
	require.NoError(t, err)

	require.Equal(t, chSourceWrapper.MdbClusterID(), connmanConnection.ClusterID)
	require.Equal(t, chSourceWrapper.User(), connmanConnection.User)
	require.Equal(t, chSourceWrapper.Password(), string(connmanConnection.Password))
	require.Equal(t, chSourceWrapper.Database(), chSource.Database)
	require.Equal(t, chSourceWrapper.SSLEnabled(), connmanConnection.HasTLS)
	require.Equal(t, chSourceWrapper.PemFileContent(), connmanConnection.CACertificates)
	require.Equal(t, chSourceWrapper.AltHosts(), connmanConnection.Hosts)
}

func TestHomogeneousSnapshotKeepsAbstract2WithExactlyOnceFlag(test *testing.T) {
	source := new(ChSource)
	for _, scenario := range []struct {
		name        string
		exactlyOnce bool
	}{
		{name: "disabled", exactlyOnce: false},
		{name: "enabled", exactlyOnce: true},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			destination := &ChDestination{ExactlyOnceSnapshot: scenario.exactlyOnce}
			transfer := model.Transfer{Src: source, Dst: destination}
			require.True(test, source.IsAbstract2(destination))
			require.True(test, transfer.IsAbstract2())
		})
	}

	otherDestination := new(model.MockDestination)
	require.False(test, source.IsAbstract2(otherDestination))
	transfer := model.Transfer{Src: source, Dst: otherDestination}
	require.False(test, transfer.IsAbstract2())
}
