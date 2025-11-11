package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultImage = "quay.io/coreos/etcd:v3.5.0"
	clientPort   = nat.Port("2379/tcp")
	peerPort     = nat.Port("2380/tcp")
)

// EtcdContainer represents a running etcd container
type EtcdContainer struct {
	testcontainers.Container
	clientPort nat.Port
}

// ClientPort returns the mapped client port
func (c *EtcdContainer) ClientPort() nat.Port {
	return c.clientPort
}

// GetEndpoint returns the etcd client endpoint URL
func (c *EtcdContainer) GetEndpoint(ctx context.Context) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	mappedPort, err := c.MappedPort(ctx, clientPort)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("http://%s:%s", host, mappedPort.Port()), nil
}

// Prepare starts an etcd container and returns a reference to it
func Prepare(ctx context.Context) (*EtcdContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        defaultImage,
		ExposedPorts: []string{clientPort.Port(), peerPort.Port()},
		WaitingFor:   wait.ForListeningPort(clientPort).WithStartupTimeout(30 * time.Second),
		Cmd: []string{
			"/usr/local/bin/etcd",
			"--advertise-client-urls", fmt.Sprintf("http://0.0.0.0:%s", clientPort.Port()),
			"--listen-client-urls", fmt.Sprintf("http://0.0.0.0:%s", clientPort.Port()),
			"--listen-peer-urls", fmt.Sprintf("http://0.0.0.0:%s", peerPort.Port()),
			"--initial-advertise-peer-urls", fmt.Sprintf("http://0.0.0.0:%s", peerPort.Port()),
			"--initial-cluster", fmt.Sprintf("default=http://0.0.0.0:%s", peerPort.Port()),
			"--initial-cluster-token", "etcd-cluster",
			"--initial-cluster-state", "new",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start etcd container: %w", err)
	}

	mappedClientPort, err := container.MappedPort(ctx, clientPort)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapped client port: %w", err)
	}

	return &EtcdContainer{
		Container:  container,
		clientPort: mappedClientPort,
	}, nil
}
