package abstract

import "context"

type WorkerType int

const (
	WorkerTypeMain         WorkerType = 0
	WorkerTypeSecondary    WorkerType = 1
	WorkerTypeSingleWorker WorkerType = 2
)

type SharedMemory interface {
	// main methods - add, get, update:

	Store(in []*OperationTablePart) error
	NextOperationTablePart(ctx context.Context) (*OperationTablePart, error)
	UpdateOperationTablesParts(operationID string, tables []*OperationTablePart) error
	Close() error

	// async things:

	GetShardStateNoWait(ctx context.Context, operationID string) (string, error)
	SetOperationState(operationID string, newState string) error
}
