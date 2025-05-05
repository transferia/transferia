package etcdcoordinator

import (
	"fmt"
	"path"
)

// --- Helper Functions ---

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
