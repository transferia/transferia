package etcdcoordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.ytsaurus.tech/library/go/core/log"

	"github.com/transferia/transferia/pkg/abstract/model"
)

// CreateOperationTablesParts creates table part entries in etcd.
func (c *EtcdCoordinator) CreateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error {
	c.logger.Info("Creating operation table parts", log.String("operationID", operationID), log.Int("partsCount", len(tables)))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, table := range tables {
		if table.TableKey() == "" {
			c.logger.Error("Attempted to create table part with empty key", log.String("operationID", operationID))
			return fmt.Errorf("table part key cannot be empty for operation %s", operationID)
		}
		key := partPath(operationID, table.TableKey())
		data, err := json.Marshal(table)
		if err != nil {
			c.logger.Error("Failed to marshal table part JSON", log.String("operationID", operationID), log.String("tableKey", table.TableKey()), log.Error(err))
			return fmt.Errorf("failed to marshal table part %s for operation %s: %w", table.TableKey(), operationID, err)
		}

		_, err = c.client.Put(ctx, key, string(data))
		if err != nil {
			c.logger.Error("Failed to put table part to etcd", log.String("key", key), log.Error(err))
			return fmt.Errorf("etcd Put failed for table part key %s: %w", key, err)
		}
	}

	c.logger.Info("Successfully created operation table parts", log.String("operationID", operationID), log.Int("partsCount", len(tables)))
	return nil
}

// GetOperationTablesParts retrieves all table parts for an operation.
func (c *EtcdCoordinator) GetOperationTablesParts(operationID string) ([]*model.OperationTablePart, error) {
	prefix := partsBasePath(operationID)
	c.logger.Debug("Getting operation table parts", log.String("operationID", operationID), log.String("prefix", prefix))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		c.logger.Error("Failed to get table parts from etcd", log.String("prefix", prefix), log.Error(err))
		return nil, fmt.Errorf("etcd Get failed for table parts prefix %s: %w", prefix, err)
	}

	tables := make([]*model.OperationTablePart, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var table model.OperationTablePart
		err = json.Unmarshal(kv.Value, &table)
		if err != nil {
			c.logger.Error("Failed to unmarshal table part JSON", log.String("key", string(kv.Key)), log.Error(err))
			return nil, fmt.Errorf("failed to unmarshal table part for key %s: %w", string(kv.Key), err)
		}
		tables = append(tables, &table)
	}

	c.logger.Debug("Successfully retrieved operation table parts", log.Int("count", len(tables)), log.String("prefix", prefix))
	return tables, nil
}

// AssignOperationTablePart assigns the first available table part to a worker.
func (c *EtcdCoordinator) AssignOperationTablePart(operationID string, workerIndex int) (*model.OperationTablePart, error) {
	prefix := partsBasePath(operationID)
	c.logger.Debug("Attempting to assign table part", log.String("operationID", operationID), log.Int("workerIndex", workerIndex), log.String("prefix", prefix))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Loop potentially needed if first attempt fails due to concurrent modification
	for attempt := 0; attempt < 3; attempt++ {
		// Get all parts
		resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			c.logger.Error("Failed to get table parts for assignment", log.String("prefix", prefix), log.Error(err))
			return nil, fmt.Errorf("etcd Get failed for assignment %s: %w", prefix, err)
		}

		var targetPart *model.OperationTablePart
		var targetKey string
		var targetRevision int64

		// Find the first unassigned part
		for _, kv := range resp.Kvs {
			var part model.OperationTablePart
			if err := json.Unmarshal(kv.Value, &part); err != nil {
				c.logger.Error("Failed to unmarshal table part during assignment", log.String("key", string(kv.Key)), log.Error(err))
				return nil, fmt.Errorf("failed to unmarshal part %s during assignment: %w", string(kv.Key), err)
			}

			if part.WorkerIndex == nil { // Found unassigned part
				targetPart = &part
				targetKey = string(kv.Key)
				targetRevision = kv.ModRevision // Get the revision for Compare
				break
			}
		}

		// If no unassigned part found, return nil
		if targetPart == nil {
			c.logger.Info("No unassigned table parts found to assign", log.String("operationID", operationID), log.Int("workerIndex", workerIndex))
			return nil, nil // No error, just nothing to assign
		}

		// Prepare the update
		targetPart.WorkerIndex = &workerIndex
		updatedData, err := json.Marshal(targetPart)
		if err != nil {
			c.logger.Error("Failed to marshal updated table part for assignment", log.String("key", targetKey), log.Error(err))
			return nil, fmt.Errorf("failed to marshal updated part %s: %w", targetKey, err)
		}

		// Attempt atomic update using Compare-and-Swap (CAS) via etcd transaction
		txnResp, err := c.client.Txn(ctx).
			// If the key targetKey still has revision targetRevision...
			If(clientv3.Compare(clientv3.ModRevision(targetKey), "=", targetRevision)).
			// Then update it with the new data
			Then(clientv3.OpPut(targetKey, string(updatedData))).
			Commit()
		if err != nil {
			c.logger.Error("Etcd transaction failed during assignment", log.String("key", targetKey), log.Error(err))
			return nil, fmt.Errorf("etcd Txn failed for assignment %s: %w", targetKey, err)
		}

		// Check if the transaction succeeded (Compare condition was met)
		if txnResp.Succeeded {
			c.logger.Info("Successfully assigned table part", log.String("key", targetKey), log.Int("workerIndex", workerIndex))
			return targetPart, nil // Assignment successful
		}

		// If transaction failed (Succeeded=false), it means the part was modified concurrently. Retry.
		c.logger.Warn("Concurrent modification detected during assignment, retrying...", log.String("key", targetKey), log.Int("attempt", attempt+1))
		time.Sleep(time.Duration(50+attempt*50) * time.Millisecond)
	}

	// If all retries fail
	c.logger.Error("Failed to assign table part after multiple retries due to conflicts", log.String("operationID", operationID), log.Int("workerIndex", workerIndex))
	return nil, fmt.Errorf("failed to assign table part for worker %d after retries", workerIndex)
}

// ClearAssignedTablesParts unassigns parts previously assigned to a specific worker.
func (c *EtcdCoordinator) ClearAssignedTablesParts(ctx context.Context, operationID string, workerIndex int) (int64, error) {
	prefix := partsBasePath(operationID)
	c.logger.Info("Clearing assigned table parts for worker", log.String("operationID", operationID), log.Int("workerIndex", workerIndex), log.String("prefix", prefix))

	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
	}

	var clearedCount int64 = 0
	var firstError error

	// Loop potentially needed if CAS fails, though less likely for clearing
	for attempt := 0; attempt < 3; attempt++ {
		firstError = nil
		clearedCount = 0

		// Get all parts
		resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			c.logger.Error("Failed to get table parts for clearing", log.String("prefix", prefix), log.Error(err))
			return 0, fmt.Errorf("etcd Get failed for clearing %s: %w", prefix, err)
		}

		var ops []clientv3.Op
		var conditions []clientv3.Cmp

		// Find parts assigned to the worker and prepare updates/conditions
		for _, kv := range resp.Kvs {
			var part model.OperationTablePart
			if err := json.Unmarshal(kv.Value, &part); err != nil {
				c.logger.Error("Failed to unmarshal table part during clearing", log.String("key", string(kv.Key)), log.Error(err))
				if firstError == nil { // Store first error
					firstError = fmt.Errorf("failed to unmarshal part %s during clearing: %w", string(kv.Key), err)
				}
				continue // Skip this part
			}

			if part.WorkerIndex != nil && *part.WorkerIndex == workerIndex {
				// Prepare update: set WorkerIndex to nil
				part.WorkerIndex = nil
				updatedData, err := json.Marshal(part)
				if err != nil {
					c.logger.Error("Failed to marshal cleared table part", log.String("key", string(kv.Key)), log.Error(err))
					if firstError == nil {
						firstError = fmt.Errorf("failed to marshal cleared part %s: %w", string(kv.Key), err)
					}
					continue // Skip this part
				}

				// Add condition: ensure the part hasn't changed since we read it
				conditions = append(conditions, clientv3.Compare(clientv3.ModRevision(string(kv.Key)), "=", kv.ModRevision))
				// Add operation: update the part
				ops = append(ops, clientv3.OpPut(string(kv.Key), string(updatedData)))
			}
		}

		// If an error occurred during unmarshal/marshal, return it now
		if firstError != nil {
			return 0, firstError
		}

		// If no parts were assigned to this worker, we're done
		if len(ops) == 0 {
			c.logger.Info("No table parts found assigned to worker to clear", log.String("operationID", operationID), log.Int("workerIndex", workerIndex))
			return 0, nil
		}

		// Execute the transaction
		txnResp, err := c.client.Txn(ctx).
			// If all parts still have their original revisions...
			If(conditions...).
			// Then apply all the updates
			Then(ops...).
			Commit()
		if err != nil {
			c.logger.Error("Etcd transaction failed during clearing", log.String("operationID", operationID), log.Int("workerIndex", workerIndex), log.Error(err))
			return 0, fmt.Errorf("etcd Txn failed for clearing parts for worker %d: %w", workerIndex, err)
		}

		// Check if the transaction succeeded
		if txnResp.Succeeded {
			clearedCount = int64(len(ops)) // Number of parts successfully cleared
			c.logger.Info("Successfully cleared assigned table parts", log.Int64("clearedCount", clearedCount), log.String("operationID", operationID), log.Int("workerIndex", workerIndex))
			return clearedCount, nil
		}

		// If transaction failed, retry
		c.logger.Warn("Concurrent modification detected during clearing, retrying...", log.String("operationID", operationID), log.Int("workerIndex", workerIndex), log.Int("attempt", attempt+1))
		time.Sleep(time.Duration(50+attempt*50) * time.Millisecond)
	}

	// If all retries fail
	c.logger.Error("Failed to clear assigned parts after multiple retries due to conflicts", log.String("operationID", operationID), log.Int("workerIndex", workerIndex))
	return 0, fmt.Errorf("failed to clear parts for worker %d after retries", workerIndex)
}

// UpdateOperationTablesParts updates specific table parts (e.g., marking as completed).
func (c *EtcdCoordinator) UpdateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error {
	c.logger.Info("Updating operation table parts", log.String("operationID", operationID), log.Int("partsToUpdate", len(tables)))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var firstError error

	var ops []clientv3.Op

	for _, table := range tables {
		if table.TableKey() == "" {
			c.logger.Error("Attempted to update table part with empty key", log.String("operationID", operationID))
			if firstError == nil {
				firstError = fmt.Errorf("cannot update table part with empty key for operation %s", operationID)
			}
			continue
		}
		key := partPath(operationID, table.TableKey())

		data, err := json.Marshal(table)
		if err != nil {
			c.logger.Error("Failed to marshal table part JSON for update", log.String("key", key), log.Error(err))
			if firstError == nil {
				firstError = fmt.Errorf("failed to marshal table part %s for update: %w", key, err)
			}
			continue
		}

		// Add Put operation to the transaction
		ops = append(ops, clientv3.OpPut(key, string(data)))
	}

	// Return marshalling errors immediately if any occurred
	if firstError != nil {
		return firstError
	}

	// Execute the transaction if there are operations
	if len(ops) > 0 {
		// Note: This simple Txn doesn't handle concurrent modifications robustly unless conditions are used.
		// If multiple agents update the same part, the last write wins without CAS.
		txnResp, err := c.client.Txn(ctx).
			Then(ops...).
			Commit()
		if err != nil {
			c.logger.Error("Etcd transaction failed during table part update", log.String("operationID", operationID), log.Error(err))
			return fmt.Errorf("etcd Txn failed for updating parts for operation %s: %w", operationID, err)
		}
		// Check txnResp.Succeeded if conditions were used
		_ = txnResp
	}

	c.logger.Info("Finished updating operation table parts", log.String("operationID", operationID), log.Int("partsAttempted", len(tables)), log.Int("partsInTxn", len(ops)))
	return nil
}
