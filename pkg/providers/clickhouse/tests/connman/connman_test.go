package connman

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	chmodel "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	source = *chrecipe.MustSource(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))
	target = *chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))

	connID = "connman_test"
)

func init() {
	source.WithDefaults()
	target.WithDefaults()
	helpers.InitConnectionResolver(map[string]connection.ManagedConnection{connID: sourceToManagedConnection(source)})
}

func TestToSinkParams(t *testing.T) {
	connSource := &chmodel.ChSource{}
	connSource.ConnectionID = connID
	sinkParamsConnman, err := connSource.ToSinkParams()
	require.NoError(t, err)

	connTarget := &chmodel.ChDestination{}
	connTarget.ConnectionID = connID
	sinkParamsConnmanTarget, err := connTarget.ToSinkParams(&model.Transfer{})
	require.NoError(t, err)

	requireSinkParamsEqual(t, sinkParamsConnman, sinkParamsConnmanTarget)

	t.Run("To sink params source", func(t *testing.T) {
		sinkParams, err := source.ToSinkParams()
		require.NoError(t, err)
		requireSinkParamsEqual(t, sinkParams, sinkParamsConnmanTarget)
	})
	t.Run("To sink params target", func(t *testing.T) {
		sinkParams, err := target.ToSinkParams(&model.Transfer{})
		require.NoError(t, err)
		requireSinkParamsEqual(t, sinkParams, sinkParamsConnmanTarget)
	})
}

func TestToStorageParams(t *testing.T) {
	connSource := &chmodel.ChSource{}
	connSource.ConnectionID = connID
	storageParams, err := connSource.ToStorageParams()
	require.NoError(t, err)

	connTarget := &chmodel.ChDestination{}
	connTarget.ConnectionID = connID
	storageParamsConnmanTarget, err := connTarget.ToStorageParams()
	require.NoError(t, err)

	require.Equal(t, storageParams.ConnectionParams, storageParamsConnmanTarget.ConnectionParams)

	t.Run("To storage params source", func(t *testing.T) {
		storageParams, err := source.ToStorageParams()
		require.NoError(t, err)
		require.Equal(t, storageParams.ConnectionParams, storageParamsConnmanTarget.ConnectionParams)
	})
	t.Run("To storage params target", func(t *testing.T) {
		storageParams, err := target.ToStorageParams()
		require.NoError(t, err)
		require.Equal(t, storageParams.ConnectionParams, storageParamsConnmanTarget.ConnectionParams)
	})
}

func sourceToManagedConnection(source chmodel.ChSource) *chconn.Connection {
	managedConn := new(chconn.Connection)
	managedConn.User = source.User
	managedConn.Password = source.Password
	managedConn.Database = source.Database
	managedConn.HasTLS = source.SSLEnabled
	managedConn.ClusterID = source.MdbClusterID
	managedConn.DatabaseNames = []string{source.Database}

	for _, shard := range source.ShardsList {
		for _, host := range shard.Hosts {
			managedConn.Hosts = append(managedConn.Hosts, &chconn.Host{
				Name:       host,
				HTTPPort:   source.HTTPPort,
				NativePort: source.NativePort,
				ShardName:  shard.Name,
			})
		}
	}

	return managedConn
}

func requireSinkParamsEqual(t *testing.T, sinkParams chmodel.ChSinkParams, expected chmodel.ChSinkParams) {
	require.Equal(t, sinkParams.User(), expected.User())
	require.Equal(t, sinkParams.Password(), expected.Password())
	require.Equal(t, sinkParams.AltHosts(), expected.AltHosts())
	require.Equal(t, sinkParams.Shards(), expected.Shards())
	require.Equal(t, sinkParams.ColumnToShardName(), expected.ColumnToShardName())
	require.Equal(t, sinkParams.Rotation(), expected.Rotation())
	require.Equal(t, sinkParams.InsertSettings(), expected.InsertSettings())
	require.Equal(t, sinkParams.MdbClusterID(), expected.MdbClusterID())
	require.Equal(t, sinkParams.Shards(), expected.Shards())
	require.Equal(t, sinkParams.ColumnToShardName(), expected.ColumnToShardName())
	require.Equal(t, sinkParams.Rotation(), expected.Rotation())
	require.Equal(t, sinkParams.InsertSettings(), expected.InsertSettings())
	require.Equal(t, sinkParams.SSLEnabled(), expected.SSLEnabled())
}
