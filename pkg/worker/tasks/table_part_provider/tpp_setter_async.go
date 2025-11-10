package table_part_provider

import (
	"context"
	"time"

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

	loadPartsCtx, cancelLoadParts := context.WithCancel(ctx)
	defer cancelLoadParts()

	// Run background loader error checker to stop asyncLoadParts by cancelling loadPartsCtx on error.
	loaderErrCh := make(chan error)
	go func() {
		defer close(loaderErrCh)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := checkLoaderError(); err != nil {
					loaderErrCh <- err
					cancelLoadParts()
					return
				}
			case <-loadPartsCtx.Done():
				return
			}
		}
	}()

	err := asyncLoadParts(
		loadPartsCtx,
		storage,
		tables,
		s.sharedMemory,
		transferID,
		operationID,
	)
	if err != nil {
		return xerrors.Errorf("unable to async load parts: %w", err)
	}

	// Async load parts finished, stopping loader error checker.
	cancelLoadParts()   // This will stop loader error checker goroutine.
	err = <-loaderErrCh // Wait for loader error.
	if err != nil {
		return xerrors.Errorf("async load parts detected loader error: %w", err)
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
