package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/worker/tasks/table_part_provider/shared_memory"
)

// To verify providers contract implementation
var (
	_ AbstractTablePartProviderSetter = (*TPPSetterSync)(nil)
)

type TPPSetterSync struct {
	sharedMemory abstract.SharedMemory
	cachedParts  []*abstract.OperationTablePart
}

func (s *TPPSetterSync) SharedMemory() abstract.SharedMemory {
	return s.sharedMemory
}

func (s *TPPSetterSync) AppendParts(ctx context.Context, parts []*abstract.OperationTablePart) error {
	if s.cachedParts != nil {
		return xerrors.Errorf("TPPSetterSync AppendParts called more than once")
	}
	s.cachedParts = parts

	tdArr, err := shared_memory.ConvertArrTablePartsToTD(s.sharedMemory, parts)
	if err != nil {
		return xerrors.Errorf("failed to convert parts: %w", err)
	}
	err = s.sharedMemory.Store(tdArr)
	if err != nil {
		return xerrors.Errorf("failed to store table parts: %w", err)
	}
	return nil
}

func (s *TPPSetterSync) AllPartsOrNil() []*abstract.OperationTablePart {
	return s.cachedParts
}

func (s *TPPSetterSync) EnrichShardedState(state string) (string, error) {
	return state, nil
}

func (s *TPPSetterSync) AsyncLoadPartsIfNeeded(
	ctx context.Context,
	storage abstract.Storage,
	tables []abstract.TableDescription,
	transferID string,
	operationID string,
	checkLoaderError func() error,
) error {
	return nil
}

func NewTPPSetterSync(sharedMemory abstract.SharedMemory) *TPPSetterSync {
	return &TPPSetterSync{
		sharedMemory: sharedMemory,
		cachedParts:  nil,
	}
}
