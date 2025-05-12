package coordinator

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

var OperationStateNotFoundError = xerrors.New("state is not found")

type TestReporter interface {
	// UpdateTestResults save transfer test results, used by test operation
	UpdateTestResults(transferID string, request *abstract.TestResult) error
}

type OperationState interface {
	// SetOperationState called by *main* worker to store coordinator info
	// for example:
	// 		postgres can store `pg_lsn` so secondary worker will read same data
	SetOperationState(taskID string, state string) error
	// GetOperationState called by *secondary* workers, so they know when to start and what to do
	GetOperationState(taskID string) (string, error)
}

type OperationStatus interface {
	// OperationHealth used by *main* and *secondary* workers to signal worker alive
	OperationHealth(ctx context.Context, operationID string, workerIndex int, workerTime time.Time) error
	// FinishOperation used by *main* and *secondary* workers to signal completeness of work
	// if worker had a failure `taskErr` will be filled up with this error
	FinishOperation(taskID, taskType string, shardIndex int, taskErr error) error
}

// Sharding coordinate multiple worker for transfer operations
// transfer utilize MPP aproach, when we have a main (or leader) worker
// main worker coordinate secondary workers via single coordinator (API or remote storage)
// main worker is usually first instance of multi-node transfer operation worker
type Sharding interface {
	// GetOperationProgress called by *main* worker to track progress over secondary workers
	GetOperationProgress(operationID string) (*model.AggregatedProgress, error)
	// CreateOperationWorkers init secondary workers state inside coordinator
	CreateOperationWorkers(operationID string, workersCount int) error
	// GetOperationWorkers return all secondary workers to *main* worker
	GetOperationWorkers(operationID string) ([]*model.OperationWorker, error)
	// GetOperationWorkersCount return number of registered secondary operation workers to *main* worker
	GetOperationWorkersCount(operationID string, completed bool) (int, error)
	// CreateOperationTablesParts store operation parts (or shards or splits).
	// each part is either full table, or some part of table defined by predicate
	// called by *main* worker
	CreateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error
	// GetOperationTablesParts return list of part needed to be uploaded
	// called by *secondary* workers
	GetOperationTablesParts(operationID string) ([]*model.OperationTablePart, error)
	// AssignOperationTablePart assign next part to defined *secondary* worker
	// each worker indexed, and use this index to assign parts
	// act as iterator.
	// if no more parts left - return `nil, nil`
	AssignOperationTablePart(operationID string, workerIndex int) (*model.OperationTablePart, error)
	// ClearAssignedTablesParts clear all assignments for worker
	// and return the number of table parts for which the assignment was cleared
	ClearAssignedTablesParts(ctx context.Context, operationID string, workerIndex int) (int64, error)
	// UpdateOperationTablesParts update tables parts for operation
	// used to track more granular part progress
	//
	// Deprecated: used only in A2
	UpdateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error
}

// Progressable is opt-in interface to show total progress over upload once completed
type Progressable interface {
	Progress() []*model.OperationTablePart
}
