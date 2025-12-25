package s3_shared_memory

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

type S3SharedMemoryMainWorker struct{}

func (m *S3SharedMemoryMainWorker) Store([]*abstract.OperationTablePart) error {
	return nil // can be called on main-worker, but result doesn't matter
}

// TAKE task
func (m *S3SharedMemoryMainWorker) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	panic("not implemented") // never should be called on main-worker
}

// COMMIT task
func (m *S3SharedMemoryMainWorker) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	panic("not implemented") // never should be called on main-worker
}

func (m *S3SharedMemoryMainWorker) Close() error {
	return nil
}

//---------------------------------------------------------------------------------------------------------------------
// metrika-trash useless things

func (m *S3SharedMemoryMainWorker) GetShardStateNoWait(ctx context.Context, operationID string) (string, error) {
	panic("not implemented")
}

func (m *S3SharedMemoryMainWorker) SetOperationState(operationID string, newState string) error {
	panic("not implemented")
}

//---------------------------------------------------------------------------------------------------------------------

func NewS3SharedMemoryMainWorker() *S3SharedMemoryMainWorker {
	return &S3SharedMemoryMainWorker{}
}
