package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

// To verify providers contract implementation
var (
	_ AbstractTablePartProviderGetter = (*TPPGetterSync)(nil)
)

type TPPGetterSync struct {
	sharedMemory abstract.SharedMemory
	transferID   string
	operationID  string
	workerIndex  int
}

func (g *TPPGetterSync) SharedMemory() abstract.SharedMemory {
	return g.sharedMemory
}

func (g *TPPGetterSync) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	return g.sharedMemory.NextOperationTablePart(ctx)
}

func NewTPPGetterSync(
	sharedMemory abstract.SharedMemory,
	transferID string,
	operationID string,
	workerIndex int,
) AbstractTablePartProviderGetter {
	return &TPPGetterSync{
		sharedMemory: sharedMemory,
		transferID:   transferID,
		operationID:  operationID,
		workerIndex:  workerIndex,
	}
}
