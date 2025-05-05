package etcdcoordinator

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.ytsaurus.tech/library/go/core/log"

	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

// EtcdConfig struct holds configuration specific to the etcd connection.
type EtcdConfig struct {
	Endpoints   []string      `yaml:"endpoints,omitempty"`
	DialTimeout time.Duration `yaml:"dial_timeout,omitempty"`
	Username    string        `yaml:"username,omitempty"`
	Password    string        `yaml:"password,omitempty"`
	CertFile    string        `yaml:"cert_file,omitempty"`
	KeyFile     string        `yaml:"key_file,omitempty"`
	CAFile      string        `yaml:"ca_file,omitempty"`
}

const (
	basePrefix       = "/transferia" // Base prefix for all transferia data in etcd
	transfersPrefix  = "transfers"
	stateSuffix      = "state"
	operationsPrefix = "operations"
	workersPrefix    = "workers"
	partsPrefix      = "parts"
)

// EtcdCoordinator implements coordinator.Sharding and coordinator.TransferState using etcd.
type EtcdCoordinator struct {
	*coordinator.CoordinatorNoOp
	client *clientv3.Client
	logger log.Logger
	config EtcdConfig
}

func NewEtcdCoordinator(ctx context.Context, config EtcdConfig, logger log.Logger) (*EtcdCoordinator, error) {
	// Input validation for config (e.g., non-empty endpoints)
	if len(config.Endpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints cannot be empty")
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}

	// Create TLS config if certificate files are provided
	tlsConfig, err := createTLSConfig(config.CertFile, config.KeyFile, config.CAFile)
	if err != nil {
		logger.Error("Failed to create TLS configuration", log.Error(err))
		return nil, fmt.Errorf("failed to create TLS configuration: %w", err)
	}

	// Create etcd client configuration
	etcdClientConfig := clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
		Username:    config.Username,
		Password:    config.Password,
		TLS:         tlsConfig,
	}

	// Create etcd client
	cli, err := clientv3.New(etcdClientConfig)
	if err != nil {
		logger.Error("Failed to connect to etcd", log.Error(err), log.Strings("endpoints", config.Endpoints))
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	logger.Info("Successfully connected to etcd", log.Strings("endpoints", config.Endpoints))

	// Return the initialized coordinator
	return &EtcdCoordinator{
		CoordinatorNoOp: &coordinator.CoordinatorNoOp{},
		client:          cli,
		logger:          log.With(logger, log.String("component", "EtcdCoordinator")),
		config:          config,
	}, nil
}

// Close terminates the connection to the etcd cluster.
func (c *EtcdCoordinator) Close() error {
	if c.client != nil {
		c.logger.Info("Closing etcd client connection")
		err := c.client.Close()
		c.client = nil // Set to nil even if close fails to prevent reuse
		if err != nil {
			c.logger.Error("Error closing etcd client", log.Error(err))
			return err
		}
		return nil
	}
	c.logger.Warn("Attempted to close already closed or uninitialized etcd client")
	return nil
}

var (
	_ coordinator.Sharding      = (*EtcdCoordinator)(nil)
	_ coordinator.TransferState = (*EtcdCoordinator)(nil)
)
