package etcdcoordinator

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.ytsaurus.tech/library/go/core/log"

	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

// --- Configuration ---

// EtcdConfig struct holds configuration specific to the etcd connection.
type EtcdConfig struct {
	Endpoints   []string      // List of etcd server endpoints
	DialTimeout time.Duration // Timeout for establishing connection
	Username    string        // Optional: Username for authentication
	Password    string        // Optional: Password for authentication
	// Add other relevant etcd client config options (TLS, etc.) if needed
}

// --- Constants ---

// Define key prefixes for different data types in etcd.
const (
	basePrefix       = "/transferia" // Base prefix for all transferia data in etcd
	transfersPrefix  = "transfers"
	stateSuffix      = "state"
	operationsPrefix = "operations"
	workersPrefix    = "workers"
	partsPrefix      = "parts"
	statusSuffix     = "status" // Potentially for operation status if not part of TransferState
	lockSuffix       = "lock"   // For potential distributed locking if needed later
)

// --- Coordinator Struct ---

// EtcdCoordinator implements coordinator.Sharding and coordinator.TransferState using etcd.
type EtcdCoordinator struct {
	*coordinator.CoordinatorNoOp
	client *clientv3.Client // etcd v3 client instance
	logger log.Logger       // Logger instance
	config EtcdConfig       // Etcd configuration
}

// --- Constructor ---

// NewEtcdCoordinator creates and initializes a new EtcdCoordinator.
// It establishes a connection to the etcd cluster.
func NewEtcdCoordinator(ctx context.Context, config EtcdConfig, logger log.Logger) (*EtcdCoordinator, error) {
	// Input validation for config (e.g., non-empty endpoints)
	if len(config.Endpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints cannot be empty")
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second // Default timeout
	}

	// Create etcd client configuration
	etcdClientConfig := clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
		Username:    config.Username,
		Password:    config.Password,
		// Add TLS config if needed
	}

	// Create etcd client
	cli, err := clientv3.New(etcdClientConfig)
	if err != nil {
		logger.Error("Failed to connect to etcd", log.Error(err), log.Strings("endpoints", config.Endpoints))
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	// Optional: Check connection with a simple Get operation or Status endpoint
	// _, err = cli.Status(ctx, config.Endpoints[0])
	// if err != nil {
	//     cli.Close()
	//     logger.Error("Failed to verify etcd connection status", log.Error(err))
	//     return nil, fmt.Errorf("failed to verify etcd connection: %w", err)
	// }

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

// --- Interface Assertions ---
// Ensure EtcdCoordinator implements the required interfaces
var (
	_ coordinator.Sharding      = (*EtcdCoordinator)(nil)
	_ coordinator.TransferState = (*EtcdCoordinator)(nil)
)
