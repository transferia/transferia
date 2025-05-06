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

// GetOperationProgress retrieves and aggregates progress from all workers.
func (c *EtcdCoordinator) GetOperationProgress(operationID string) (*model.AggregatedProgress, error) {
	c.logger.Debug("Getting operation progress", log.String("operationID", operationID))
	workers, err := c.GetOperationWorkers(operationID)
	if err != nil {
		c.logger.Error("Failed to get workers for progress aggregation", log.String("operationID", operationID), log.Error(err))
		return nil, fmt.Errorf("failed to get workers for progress: %w", err)
	}

	// Aggregate progress from workers
	aggregatedProgress := model.NewAggregatedProgress()
	for _, worker := range workers {
		if worker.Progress != nil {
			// Add worker progress to aggregatedProgress
			aggregatedProgress.PartsCount += worker.Progress.PartsCount
			aggregatedProgress.CompletedPartsCount += worker.Progress.CompletedPartsCount
			aggregatedProgress.ETARowsCount += worker.Progress.ETARowsCount
			aggregatedProgress.CompletedRowsCount += worker.Progress.CompletedRowsCount
			aggregatedProgress.TotalReadBytes += worker.Progress.TotalReadBytes
			// Duration and LastUpdateAt need careful aggregation (e.g., max LastUpdateAt)
			if worker.Progress.LastUpdateAt.After(aggregatedProgress.LastUpdateAt) {
				aggregatedProgress.LastUpdateAt = worker.Progress.LastUpdateAt
			}
			// TotalDuration might be sum or max depending on meaning
			aggregatedProgress.TotalDuration += worker.Progress.TotalDuration
		}
	}

	c.logger.Debug("Calculated aggregated progress", log.String("operationID", operationID))
	return aggregatedProgress, nil
}

// CreateOperationWorkers creates worker entries in etcd.
func (c *EtcdCoordinator) CreateOperationWorkers(operationID string, workersCount int) error {
	c.logger.Info("Creating operation workers", log.String("operationID", operationID), log.Int("workersCount", workersCount))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 1; i <= workersCount; i++ {
		worker := &model.OperationWorker{
			OperationID: operationID,
			WorkerIndex: i,
			Completed:   false,
			Err:         "",
			Progress:    model.NewAggregatedProgress(),
		}
		worker.Progress.LastUpdateAt = time.Now()

		key := workerPath(operationID, i)
		data, err := json.Marshal(worker)
		if err != nil {
			c.logger.Error("Failed to marshal worker JSON", log.String("operationID", operationID), log.Int("workerIndex", i), log.Error(err))
			return fmt.Errorf("failed to marshal worker %d for operation %s: %w", i, operationID, err)
		}

		_, err = c.client.Put(ctx, key, string(data))
		if err != nil {
			c.logger.Error("Failed to put worker to etcd", log.String("key", key), log.Error(err))
			return fmt.Errorf("etcd Put failed for worker key %s: %w", key, err)
		}
	}

	c.logger.Info("Successfully created operation workers", log.String("operationID", operationID), log.Int("workersCount", workersCount))
	return nil
}

// GetOperationWorkers retrieves all workers for an operation.
func (c *EtcdCoordinator) GetOperationWorkers(operationID string) ([]*model.OperationWorker, error) {
	prefix := workersBasePath(operationID)
	c.logger.Debug("Getting operation workers", log.String("operationID", operationID), log.String("prefix", prefix))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		c.logger.Error("Failed to get workers from etcd", log.String("prefix", prefix), log.Error(err))
		return nil, fmt.Errorf("etcd Get failed for workers prefix %s: %w", prefix, err)
	}

	workers := make([]*model.OperationWorker, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var worker model.OperationWorker
		err = json.Unmarshal(kv.Value, &worker)
		if err != nil {
			c.logger.Error("Failed to unmarshal worker JSON", log.String("key", string(kv.Key)), log.Error(err))
			return nil, fmt.Errorf("failed to unmarshal worker for key %s: %w", string(kv.Key), err)
		}
		workers = append(workers, &worker)
	}

	c.logger.Debug("Successfully retrieved operation workers", log.Int("count", len(workers)), log.String("prefix", prefix))
	return workers, nil
}

// GetOperationWorkersCount retrieves the count of workers (completed or not).
func (c *EtcdCoordinator) GetOperationWorkersCount(operationID string, completed bool) (int, error) {
	c.logger.Debug("Getting operation workers count", log.String("operationID", operationID), log.Bool("completed", completed))
	workers, err := c.GetOperationWorkers(operationID)
	if err != nil {
		return 0, err // Error already logged by GetOperationWorkers
	}

	count := 0
	for _, worker := range workers {
		if worker.Completed == completed {
			count++
		}
	}
	c.logger.Debug("Calculated workers count", log.Int("count", count), log.String("operationID", operationID), log.Bool("completed", completed))
	return count, nil
}

// FinishOperation marks a specific worker as completed.
func (c *EtcdCoordinator) FinishOperation(operationID string, shardIndex int, taskErr error) error {
	key := workerPath(operationID, shardIndex)
	c.logger.Info("Finishing operation for worker", log.String("operationID", operationID), log.Int("workerIndex", shardIndex), log.String("key", key), log.Error(taskErr))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for attempt := 0; attempt < 3; attempt++ {
		// Get the current worker state
		resp, err := c.client.Get(ctx, key)
		if err != nil {
			c.logger.Error("Failed to get worker for finishing", log.String("key", key), log.Error(err))
			return fmt.Errorf("etcd Get failed for finishing worker %s: %w", key, err)
		}

		if len(resp.Kvs) == 0 {
			c.logger.Error("Worker not found for finishing", log.String("key", key))
			return fmt.Errorf("worker %d not found for operation %s", shardIndex, operationID) // Error if worker doesn't exist
		}

		kv := resp.Kvs[0]
		var worker model.OperationWorker
		if err := json.Unmarshal(kv.Value, &worker); err != nil {
			c.logger.Error("Failed to unmarshal worker JSON for finishing", log.String("key", key), log.Error(err))
			return fmt.Errorf("failed to unmarshal worker %s for finishing: %w", key, err)
		}

		// Update the worker state
		worker.Completed = true
		worker.Progress.LastUpdateAt = time.Now()
		if taskErr != nil {
			worker.Err = taskErr.Error()
		} else {
			worker.Err = "" // Clear previous error if finished successfully
		}

		updatedData, err := json.Marshal(worker)
		if err != nil {
			c.logger.Error("Failed to marshal updated worker JSON for finishing", log.String("key", key), log.Error(err))
			return fmt.Errorf("failed to marshal updated worker %s: %w", key, err)
		}

		// Attempt atomic update using CAS
		txnResp, err := c.client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.ModRevision)).
			Then(clientv3.OpPut(key, string(updatedData))).
			Commit()
		if err != nil {
			c.logger.Error("Etcd transaction failed during finishing worker", log.String("key", key), log.Error(err))
			return fmt.Errorf("etcd Txn failed for finishing worker %s: %w", key, err)
		}

		// Check if succeeded
		if txnResp.Succeeded {
			c.logger.Info("Successfully finished operation for worker", log.String("key", key), log.Int("workerIndex", shardIndex))
			return nil // Success
		}

		// Retry on conflict
		c.logger.Warn("Concurrent modification detected during finishing worker, retrying...", log.String("key", key), log.Int("attempt", attempt+1))
		time.Sleep(time.Duration(50+attempt*50) * time.Millisecond)
	}

	// If all retries fail
	c.logger.Error("Failed to finish worker after multiple retries due to conflicts", log.String("key", key))
	return fmt.Errorf("failed to finish worker %d after retries", shardIndex)
}
