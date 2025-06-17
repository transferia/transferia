//go:build !disable_s3_coordinator

package s3coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	_ coordinator.Sharding        = (*CoordinatorS3)(nil)
	_ coordinator.TransferState   = (*CoordinatorS3)(nil)
	_ coordinator.OperationStatus = (*CoordinatorS3)(nil)
)

type CoordinatorS3 struct {
	*coordinator.CoordinatorNoOp

	mu       sync.Mutex
	state    map[string]map[string]*coordinator.TransferStateData
	s3Client *s3.S3
	bucket   string
	lgr      log.Logger
}

// GetTransferState fetches all state objects with a given transferID (prefix).
func (c *CoordinatorS3) GetTransferState(transferID string) (map[string]*coordinator.TransferStateData, error) {
	state := make(map[string]*coordinator.TransferStateData)

	// List objects with the prefix transferID/
	prefix := transferID + "/"
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}
	listResp, err := c.s3Client.ListObjectsV2(listInput)
	if err != nil {
		return nil, xerrors.Errorf("failed to list objects: %w", err)
	}

	// Fetch each object and deserialize the JSON
	for _, obj := range listResp.Contents {
		if obj.Size == nil || *obj.Size == 0 {
			// see: https://stackoverflow.com/questions/75620230/aws-s3-listobjectsv2-returns-folder-as-an-object
			continue
		}
		key := strings.TrimPrefix(*obj.Key, prefix)
		getInput := &s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    obj.Key,
		}
		resp, err := c.s3Client.GetObject(getInput)
		if err != nil {
			return nil, xerrors.Errorf("failed to get object: %w", err)
		}
		defer resp.Body.Close()

		var transferData coordinator.TransferStateData
		if err := json.NewDecoder(resp.Body).Decode(&transferData); err != nil {
			return nil, xerrors.Errorf("failed to decode object data: %w", err)
		}
		state[strings.TrimSuffix(key, ".json")] = &transferData
	}
	c.lgr.Info("load transfer state", log.Any("transfer_id", transferID), log.Any("state", state))

	return state, nil
}

// SetTransferState stores the given transfer state into S3 as JSON objects.
func (c *CoordinatorS3) SetTransferState(transferID string, state map[string]*coordinator.TransferStateData) error {
	for key, value := range state {
		objectKey := transferID + "/" + key + ".json"
		body, err := json.Marshal(value)
		if err != nil {
			return xerrors.Errorf("failed to marshal state data: %w", err)
		}

		_, err = c.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(body),
		})
		if err != nil {
			return xerrors.Errorf("failed to upload state object: %w", err)
		}
	}
	c.lgr.Info("set transfer state", log.Any("transfer_id", transferID), log.Any("state", state))
	return nil
}

// RemoveTransferState removes the specified state keys for a given transferID from S3.
func (c *CoordinatorS3) RemoveTransferState(transferID string, keys []string) error {
	for _, key := range keys {
		objectKey := transferID + "/" + key + ".json"

		_, err := c.s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			return xerrors.Errorf("failed to delete state object: %w", err)
		}
	}
	c.lgr.Info("remove transfer state keys", log.Any("transfer_id", transferID), log.Any("keys", keys))
	return nil
}

// GetOperationProgress do nothing
func (c *CoordinatorS3) GetOperationProgress(operationID string) (*model.AggregatedProgress, error) {
	return model.NewAggregatedProgress(), nil
}

// CreateOperationWorkers creates worker files with initial progress for the operation.
func (c *CoordinatorS3) CreateOperationWorkers(operationID string, workersCount int) error {
	for i := 1; i <= workersCount; i++ {
		worker := &model.OperationWorker{
			OperationID: operationID,
			WorkerIndex: i,
			Completed:   false,
			Err:         "",
			Progress: &model.AggregatedProgress{
				PartsCount:          0,
				CompletedPartsCount: 0,
				ETARowsCount:        0,
				CompletedRowsCount:  0,
				TotalReadBytes:      0,
				TotalDuration:       0,
				LastUpdateAt:        time.Now(),
			},
		}

		key := fmt.Sprintf("%s/worker_%d.json", operationID, i)
		body, err := json.Marshal(worker)
		if err != nil {
			return xerrors.Errorf("failed to marshal worker: %w", err)
		}

		if err := c.putObject(key, body); err != nil {
			return xerrors.Errorf("failed to create worker: %w", err)
		}
	}
	return nil
}

// GetOperationWorkers fetches all workers for the given operationID.
func (c *CoordinatorS3) GetOperationWorkers(operationID string) ([]*model.OperationWorker, error) {
	prefix := operationID + "/worker_"
	objects, err := c.listObjects(prefix)
	if err != nil {
		return nil, xerrors.Errorf("failed to list prefix: %s: %w", prefix, err)
	}

	var workers []*model.OperationWorker
	for _, obj := range objects {
		resp, err := c.getObject(*obj.Key)
		if err != nil {
			return nil, xerrors.Errorf("failed to get key: %s: %w", *obj.Key, err)
		}

		var worker model.OperationWorker
		if err := json.Unmarshal(resp, &worker); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal worker: %w", err)
		}
		workers = append(workers, &worker)
	}

	return workers, nil
}

// GetOperationWorkersCount returns the count of workers either completed or not completed.
func (c *CoordinatorS3) GetOperationWorkersCount(operationID string, completed bool) (int, error) {
	workers, err := c.GetOperationWorkers(operationID)
	if err != nil {
		return 0, err
	}

	return len(workers), nil
}

// CreateOperationTablesParts creates table parts for an operation and stores them in S3.
func (c *CoordinatorS3) CreateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error {
	for _, table := range tables {
		key := fmt.Sprintf("%s/table_%v.json", operationID, table.TableKey())

		body, err := json.Marshal(table)
		if err != nil {
			return xerrors.Errorf("failed to marshal table part: %w", err)
		}

		if err := c.putObject(key, body); err != nil {
			return xerrors.Errorf("failed to create table part: %w", err)
		}
	}
	return nil
}

// GetOperationTablesParts fetches table parts for a given operation.
func (c *CoordinatorS3) GetOperationTablesParts(operationID string) ([]*model.OperationTablePart, error) {
	prefix := operationID + "/table_"
	objects, err := c.listObjects(prefix)
	if err != nil {
		return nil, xerrors.Errorf("failed to list: %s: %w", prefix, err)
	}

	var tables []*model.OperationTablePart
	for _, obj := range objects {
		resp, err := c.getObject(*obj.Key)
		if err != nil {
			return nil, xerrors.Errorf("failed to get: %s: %w", *obj.Key, err)
		}

		var table model.OperationTablePart
		if err := json.Unmarshal(resp, &table); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal table part: %w", err)
		}
		tables = append(tables, &table)
	}

	return tables, nil
}

// AssignOperationTablePart assigns a table part to a worker.
func (c *CoordinatorS3) AssignOperationTablePart(operationID string, workerIndex int) (*model.OperationTablePart, error) {
	tables, err := c.GetOperationTablesParts(operationID)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for _, table := range tables {
		lastErr = nil
		if table.WorkerIndex != nil {
			continue
		}
		table.WorkerIndex = &workerIndex

		key := fmt.Sprintf("%s/table_%v.json", operationID, table.TableKey())
		body, err := json.Marshal(table)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal table part: %w", err)
		}

		if err := c.putObject(key, body); err != nil {
			lastErr = err
			logger.Log.Warn(fmt.Sprintf("failed to assign: %s to %v", key, workerIndex), log.Error(err))
			continue
		}
		return table, nil
	}
	if lastErr != nil {
		return nil, xerrors.Errorf("failed to assign: %w", lastErr)
	}
	// nothing to assign
	return nil, nil
}

// ClearAssignedTablesParts clears the table parts assigned to a worker.
func (c *CoordinatorS3) ClearAssignedTablesParts(ctx context.Context, operationID string, workerIndex int) (int64, error) {
	tables, err := c.GetOperationTablesParts(operationID)
	if err != nil {
		return 0, err
	}

	var clearedCount int64
	for _, table := range tables {
		if table.WorkerIndex != nil && *table.WorkerIndex == workerIndex {
			table.WorkerIndex = nil

			key := fmt.Sprintf("%s/table_%v.json", operationID, table.TableKey())
			body, err := json.Marshal(table)
			if err != nil {
				return 0, xerrors.Errorf("failed to marshal table part: %w", err)
			}

			if err := c.putObject(key, body); err != nil {
				return 0, xerrors.Errorf("failed to update table part: %w", err)
			}
			clearedCount++
		}
	}
	return clearedCount, nil
}

// UpdateOperationTablesParts updates the status of table parts in S3.
func (c *CoordinatorS3) UpdateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error {
	currentTables, err := c.GetOperationTablesParts(operationID)
	if err != nil {
		return xerrors.Errorf("failed to get tables parts: %w", err)
	}
	curTbls := map[string]*model.OperationTablePart{}
	for _, table := range currentTables {
		curTbls[table.TableKey()] = table
	}

	for _, table := range tables {
		existTbl, ok := curTbls[table.TableKey()]
		if ok {
			table.WorkerIndex = existTbl.WorkerIndex
		}
		key := fmt.Sprintf("%s/table_%v.json", operationID, table.TableKey())
		body, err := json.Marshal(table)
		if err != nil {
			return xerrors.Errorf("failed to marshal table part: %w", err)
		}

		if err := c.putObject(key, body); err != nil {
			return xerrors.Errorf("failed to update table part: %w", err)
		}
	}
	return nil
}

func (c *CoordinatorS3) FinishOperation(operationID string, taskType string, shardIndex int, taskErr error) error {
	workers, err := c.GetOperationWorkers(operationID)
	if err != nil {
		return xerrors.Errorf("failed to load operation parts: %w", err)
	}
	for _, worker := range workers {
		if worker.WorkerIndex != shardIndex {
			continue
		}
		worker.Completed = true
		if taskErr != nil {
			worker.Err = taskErr.Error()
		}
		key := fmt.Sprintf("%s/worker_%d.json", operationID, shardIndex)
		body, err := json.Marshal(worker)
		if err != nil {
			return xerrors.Errorf("failed to marshal worker: %w", err)
		}

		if err := c.putObject(key, body); err != nil {
			return xerrors.Errorf("failed to create worker: %w", err)
		}
	}

	return nil
}

// Utility functions to interact with S3.
func (c *CoordinatorS3) putObject(key string, body []byte) error {
	_, err := c.s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	return err
}

func (c *CoordinatorS3) getObject(key string) ([]byte, error) {
	resp, err := c.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get: %s: key: %w", key, err)
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (c *CoordinatorS3) listObjects(prefix string) ([]*s3.Object, error) {
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}
	listResp, err := c.s3Client.ListObjectsV2(listInput)
	if err != nil {
		return nil, xerrors.Errorf("failed to list objects: %w", err)
	}
	return listResp.Contents, nil
}

// NewS3 creates a new CoordinatorS3 with AWS SDK v1.
func NewS3(bucket string, l log.Logger, cfgs ...*aws.Config) (*CoordinatorS3, error) {
	sess, err := session.NewSession(cfgs...)
	if err != nil {
		return nil, xerrors.Errorf("unable to create AWS session: %w", err)
	}

	// Create the S3 client using the session.
	s3Client := s3.New(sess)

	// Return the CoordinatorS3 instance.
	return &CoordinatorS3{
		CoordinatorNoOp: coordinator.NewFakeClient(), // Assuming this is a valid function in your code.
		mu:              sync.Mutex{},
		state:           map[string]map[string]*coordinator.TransferStateData{},
		bucket:          bucket,
		s3Client:        s3Client,
		lgr:             log.With(l, log.Any("component", "s3-coordinator")),
	}, nil
}
