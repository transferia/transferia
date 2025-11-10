package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// To verify providers contract implementation
var (
	_ AbstractTablePartProviderSetter = (*TPPSetterSync)(nil)
)

type TPPSetterSync struct {
	sharedMemory abstract.SharedMemory
	cachedParts  []*abstract.OperationTablePart
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

func NewTPPSetterSync(sharedMemory abstract.SharedMemory, parts []*abstract.OperationTablePart) (*TPPSetterSync, error) {
	setter := &TPPSetterSync{
		sharedMemory: sharedMemory,
		cachedParts:  parts,
	}
	if err := setter.sharedMemory.Store(parts); err != nil {
		return nil, xerrors.Errorf("failed to store table parts: %w", err)
	}
	return setter, nil
}
