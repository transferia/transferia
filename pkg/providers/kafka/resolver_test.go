package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dbaas"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
)

// MockHostResolver implements dbaas.HostResolver for testing
type MockHostResolver struct {
	hosts []dbaas.ClusterHost
	err   error
}

func (m *MockHostResolver) ResolveHosts() ([]dbaas.ClusterHost, error) {
	return m.hosts, m.err
}

// MockResolverFactory implements dbaas.ResolverFactory for testing
type MockResolverFactory struct {
	resolvers map[string]dbaas.HostResolver
}

func (m *MockResolverFactory) HostResolver(typ dbaas.ProviderType, clusterID string) (dbaas.HostResolver, error) {
	resolver, ok := m.resolvers[clusterID]
	if !ok {
		return nil, xerrors.Errorf("cluster %s not found", clusterID)
	}
	return resolver, nil
}

func (m *MockResolverFactory) PasswordResolver(typ dbaas.ProviderType, clusterID string) (dbaas.PasswordResolver, error) {
	return nil, dbaas.NotSupported
}

func (m *MockResolverFactory) ShardResolver(typ dbaas.ProviderType, clusterID string) (dbaas.ShardResolver, error) {
	return nil, dbaas.NotSupported
}

func (m *MockResolverFactory) ShardGroupHostsResolver(typ dbaas.ProviderType, clusterID string) (dbaas.ShardGroupHostsResolver, error) {
	return nil, dbaas.NotSupported
}

func TestResolveBrokers(t *testing.T) {
	// Setup mock dbaas provider
	mockFactory := &MockResolverFactory{
		resolvers: make(map[string]dbaas.HostResolver),
	}

	// Initialize dbaas with our mock
	dbaas.Init(mockFactory)

	t.Run("should return fatal error when cluster does not exist", func(t *testing.T) {
		// Arrange
		clusterID := "non-existent-cluster"
		opts := &KafkaConnectionOptions{
			ClusterID: clusterID,
		}
		mockResolver := &MockHostResolver{
			hosts: []dbaas.ClusterHost{},
			err:   abstract.NewFatalError(coded.Errorf(codes.MDBNotFound, "cluster %s not found", clusterID)),
		}
		mockFactory.resolvers[clusterID] = mockResolver

		// Act
		brokers, err := ResolveBrokers(opts)

		// Assert
		require.Error(t, err)
		require.True(t, abstract.IsFatal(err), "error should be fatal, transiential through `ResolveBrokers` function")
		require.Nil(t, brokers)
		require.Contains(t, err.Error(), "cannot get hosts for cluster")
	})

	t.Run("should return non-fatal error when all hosts are dead", func(t *testing.T) {
		// Arrange
		clusterID := "cluster-with-dead-hosts"
		mockResolver := &MockHostResolver{
			hosts: []dbaas.ClusterHost{
				// basically, ClusterHost implementation do not return DEAD hosts, so the slice should be empty
				// {Name: "host1:9091", Health: "DEAD"},
				// {Name: "host2:9091", Health: "DEAD"},
			},
			err: nil,
		}
		mockFactory.resolvers[clusterID] = mockResolver

		opts := &KafkaConnectionOptions{
			ClusterID: clusterID,
		}

		// Act
		brokers, err := ResolveBrokers(opts)

		// Assert
		require.Error(t, err)
		require.False(t, abstract.IsFatal(err), "error should NOT be fatal when all hosts are dead")
		require.Nil(t, brokers)
		require.Contains(t, err.Error(), "unable to connect, no brokers found")
	})

	t.Run("should return non-fatal error when both ClusterID and Brokers are empty", func(t *testing.T) {
		// Arrange
		opts := &KafkaConnectionOptions{
			ClusterID: "",
			Brokers:   []string{},
		}

		// Act
		brokers, err := ResolveBrokers(opts)

		// Assert
		require.Error(t, err)
		require.False(t, abstract.IsFatal(err), "error should NOT be fatal when both ClusterID and Brokers are empty")
		require.Nil(t, brokers)
		require.Contains(t, err.Error(), "unable to connect, no brokers found")
	})

	t.Run("should return brokers when ClusterID is provided and hosts are alive", func(t *testing.T) {
		// Arrange
		clusterID := "cluster-with-alive-hosts"
		mockResolver := &MockHostResolver{
			hosts: []dbaas.ClusterHost{
				{Name: "host1:9091", Health: dbaas.ALIVE},
				{Name: "host2:9091", Health: dbaas.ALIVE},
			},
		}
		mockFactory.resolvers[clusterID] = mockResolver

		opts := &KafkaConnectionOptions{
			ClusterID: clusterID,
		}

		// Act
		brokers, err := ResolveBrokers(opts)

		// Assert
		require.NoError(t, err)
		require.Equal(t, []string{"host1:9091", "host2:9091"}, brokers)
	})

	t.Run("should return brokers when direct Brokers are provided", func(t *testing.T) {
		// Arrange
		opts := &KafkaConnectionOptions{
			ClusterID: "",
			Brokers:   []string{"broker1:9092", "broker2:9092"},
		}

		// Act
		brokers, err := ResolveBrokers(opts)

		// Assert
		require.NoError(t, err)
		require.Equal(t, []string{"broker1:9092", "broker2:9092"}, brokers)
	})

	t.Run("should return error when empty direct Brokers are provided", func(t *testing.T) {
		// Arrange
		opts := &KafkaConnectionOptions{
			ClusterID: "",
			Brokers:   []string{},
		}

		// Act
		brokers, err := ResolveBrokers(opts)

		// Assert
		require.Error(t, err)
		require.Nil(t, brokers)
		require.Contains(t, err.Error(), "unable to connect, no brokers found")
	})

	t.Run("should prefer ClusterID over Brokers when both are provided", func(t *testing.T) {
		// Arrange
		clusterID := "cluster-with-alive-hosts"
		mockResolver := &MockHostResolver{
			hosts: []dbaas.ClusterHost{
				{Name: "cluster-host:9091", Health: dbaas.ALIVE},
			},
		}
		mockFactory.resolvers[clusterID] = mockResolver

		opts := &KafkaConnectionOptions{
			ClusterID: clusterID,
			Brokers:   []string{"direct-broker:9092"},
		}

		// Act
		brokers, err := ResolveBrokers(opts)

		// Assert
		require.NoError(t, err)
		require.Equal(t, []string{"cluster-host:9091"}, brokers)
	})

	t.Run("should handle mixed alive and dead hosts correctly", func(t *testing.T) {
		// Arrange
		clusterID := "cluster-with-mixed-hosts"
		mockResolver := &MockHostResolver{
			hosts: []dbaas.ClusterHost{
				{Name: "host1:9091", Health: dbaas.ALIVE},
				{Name: "host2:9091", Health: "DEAD"},
				{Name: "host3:9091", Health: dbaas.ALIVE},
			},
		}
		mockFactory.resolvers[clusterID] = mockResolver

		opts := &KafkaConnectionOptions{
			ClusterID: clusterID,
		}

		// Act
		brokers, err := ResolveBrokers(opts)

		// Assert
		require.NoError(t, err)
		// Note: The current implementation returns ALL hosts regardless of health status
		require.Equal(t, []string{"host1:9091", "host2:9091", "host3:9091"}, brokers)
	})
}
