package etcdcoordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.ytsaurus.tech/library/go/core/log"

	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

// GetTransferState retrieves all state objects for a given transferID.
// Maps to etcd Get operation with prefix.
func (c *EtcdCoordinator) GetTransferState(transferID string) (map[string]*coordinator.TransferStateData, error) {
	prefix := transferStatePrefix(transferID)
	c.logger.Debug("Getting transfer state", log.String("transferID", transferID), log.String("prefix", prefix))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		c.logger.Error("Failed to get transfer state from etcd", log.String("transferID", transferID), log.String("prefix", prefix), log.Error(err))
		return nil, fmt.Errorf("etcd Get failed for transfer state prefix %s: %w", prefix, err)
	}

	state := make(map[string]*coordinator.TransferStateData)
	if len(resp.Kvs) == 0 {
		c.logger.Info("No transfer state found", log.String("transferID", transferID), log.String("prefix", prefix))
		return state, nil // Return empty map if not found
	}

	for _, kv := range resp.Kvs {
		var transferData coordinator.TransferStateData
		err = json.Unmarshal(kv.Value, &transferData)
		if err != nil {
			c.logger.Error("Failed to unmarshal transfer state JSON", log.String("transferID", transferID), log.String("key", string(kv.Key)), log.Error(err))
			return nil, fmt.Errorf("failed to unmarshal transfer state for key %s: %w", string(kv.Key), err)
		}
		// Extract the original key name from the etcd key
		originalKey := strings.TrimSuffix(path.Base(string(kv.Key)), ".json")
		state[originalKey] = &transferData
	}

	c.logger.Info("Successfully retrieved transfer state", log.String("transferID", transferID), log.Int("keysFound", len(state)))
	return state, nil
}

// SetTransferState saves the state map for a given transferID.
// Maps to multiple etcd Put operations.
func (c *EtcdCoordinator) SetTransferState(transferID string, state map[string]*coordinator.TransferStateData) error {
	c.logger.Debug("Setting transfer state", log.String("transferID", transferID), log.Int("keysToSet", len(state)))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for key, value := range state {
		etcdKey := transferStatePath(transferID, key)
		data, err := json.Marshal(value)
		if err != nil {
			c.logger.Error("Failed to marshal transfer state JSON", log.String("transferID", transferID), log.String("stateKey", key), log.Error(err))
			return fmt.Errorf("failed to marshal transfer state for key %s: %w", key, err)
		}

		_, err = c.client.Put(ctx, etcdKey, string(data))
		if err != nil {
			c.logger.Error("Failed to put transfer state to etcd", log.String("transferID", transferID), log.String("etcdKey", etcdKey), log.Error(err))
			return fmt.Errorf("etcd Put failed for transfer state key %s: %w", etcdKey, err)
		}
	}

	c.logger.Info("Successfully set transfer state", log.String("transferID", transferID), log.Int("keysSet", len(state)))
	return nil
}

// RemoveTransferState removes specific state keys for a transferID.
// Maps to multiple etcd Delete operations.
func (c *EtcdCoordinator) RemoveTransferState(transferID string, keys []string) error {
	c.logger.Info("Removing transfer state keys", log.String("transferID", transferID), log.Strings("keys", keys))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var firstError error
	deletedCount := 0

	for _, key := range keys {
		etcdKey := transferStatePath(transferID, key)
		resp, err := c.client.Delete(ctx, etcdKey)
		if err != nil {
			c.logger.Error("Failed to delete transfer state key from etcd", log.String("etcdKey", etcdKey), log.Error(err))
			// Store the first error encountered, but try to delete others
			if firstError == nil {
				firstError = fmt.Errorf("etcd Delete failed for key %s: %w", etcdKey, err)
			}
			continue // Try next key
		}
		if resp.Deleted > 0 {
			deletedCount += int(resp.Deleted)
		} else {
			c.logger.Warn("Attempted to remove non-existent transfer state key", log.String("etcdKey", etcdKey))
		}
	}

	if firstError != nil {
		return firstError // Return the first error encountered
	}

	c.logger.Info("Finished removing transfer state keys", log.String("transferID", transferID), log.Int("keysAttempted", len(keys)), log.Int("keysActuallyDeleted", deletedCount))
	return nil
}
