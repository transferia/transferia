package etcdcoordinator

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path"
)

// transferBasePath returns the base path for a transfer in etcd.
func transferBasePath(transferID string) string {
	return path.Join(basePrefix, transfersPrefix, transferID)
}

// transferStatePath returns the path for a specific transfer state key.
func transferStatePath(transferID, key string) string {
	return path.Join(transferBasePath(transferID), stateSuffix, key+".json")
}

// transferStatePrefix returns the prefix for listing all state keys for a transfer.
func transferStatePrefix(transferID string) string {
	return path.Join(transferBasePath(transferID), stateSuffix) + "/"
}

// operationBasePath returns the base path for an operation in etcd.
func operationBasePath(operationID string) string {
	return path.Join(basePrefix, operationsPrefix, operationID)
}

// workersBasePath returns the prefix for listing all workers for an operation.
func workersBasePath(operationID string) string {
	return path.Join(operationBasePath(operationID), workersPrefix) + "/"
}

// workerPath returns the path for a specific worker in etcd.
func workerPath(operationID string, workerIndex int) string {
	return path.Join(operationBasePath(operationID), workersPrefix, fmt.Sprintf("worker_%d.json", workerIndex))
}

// partsBasePath returns the prefix for listing all table parts for an operation.
func partsBasePath(operationID string) string {
	return path.Join(operationBasePath(operationID), partsPrefix) + "/"
}

// partPath returns the path for a specific table part in etcd.
func partPath(operationID, tableKey string) string {
	return path.Join(operationBasePath(operationID), partsPrefix, fmt.Sprintf("table_%s.json", tableKey))
}

// createTLSConfig creates a TLS configuration for etcd client.
// If no certificate files are provided, it returns nil config and nil error.
func createTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" && caFile == "" {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Load CA certificate if provided
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate and key if both are provided
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate and key: %w", err)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	} else if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		// If only one of cert or key is provided, return an error
		return nil, fmt.Errorf("both certificate and key files must be provided together")
	}

	return tlsConfig, nil
}
