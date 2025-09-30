package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// To verify providers contract implementation
var (
	_ AbstractTablePartProviderSetter = (*TPPSetterAsync)(nil)
)

type TPPSetterAsync struct {
	sharedMemory abstract.SharedMemory
}

func (s *TPPSetterAsync) SharedMemory() abstract.SharedMemory {
	return s.sharedMemory
}

func (s *TPPSetterAsync) AppendParts(ctx context.Context, parts []*abstract.OperationTablePart) error {
	return nil
}

func (s *TPPSetterAsync) AllPartsOrNil() []*abstract.OperationTablePart {
	return nil
}

func (s *TPPSetterAsync) EnrichShardedState(inState string) (string, error) {
	newState, err := addKeyToJson(inState, abstract.IsAsyncPartsUploadedStateKey, false)
	if err != nil {
		return "", xerrors.Errorf("unable to add key to state: %w", err)
	}
	return string(newState), nil
}

func (s *TPPSetterAsync) AsyncLoadPartsIfNeeded(
	ctx context.Context,
	inStorage abstract.Storage,
	tables []abstract.TableDescription,
	transferID string,
	operationID string,
	checkLoaderError func() error,
) error {
	storage, ok := inStorage.(abstract.NextArrTableDescriptionGetterBuilder)
	if !ok {
		return xerrors.New("storage does not implement AsyncLoadPartsIfNeeded")
	}

	err := asyncLoadParts(
		ctx,
		storage,
		tables,
		s.sharedMemory,
		transferID,
		operationID,
		func() error {
			return checkLoaderError()
		},
	)
	if err != nil {
		return xerrors.Errorf("unable to async load parts: %w", err)
	}

	// mark shareded_state by flag

	shardedStateBytes, err := storage.ShardingContext()
	if err != nil {
		return xerrors.Errorf("unable to prepare sharded state for operation '%v': %w", operationID, err)
	}
	newState, err := addKeyToJson(string(shardedStateBytes), abstract.IsAsyncPartsUploadedStateKey, true)
	if err != nil {
		return xerrors.Errorf("unable to add key to state: %w", err)
	}
	err = s.sharedMemory.SetOperationState(operationID, string(newState))
	if err != nil {
		return xerrors.Errorf("unable to set sharded state after upload: %w", err)
	}
	return nil
}

func NewTPPSetterAsync(sharedMemory abstract.SharedMemory) *TPPSetterAsync {
	return &TPPSetterAsync{
		sharedMemory: sharedMemory,
	}
}
