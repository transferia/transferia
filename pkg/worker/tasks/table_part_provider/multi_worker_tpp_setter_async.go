package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

type MultiWorkerTPPSetterAsync struct {
}

func (s *MultiWorkerTPPSetterAsync) AppendParts(ctx context.Context, parts []*abstract.OperationTablePart) error {
	return nil
}

func (s *MultiWorkerTPPSetterAsync) EnrichShardedState(inState string) (string, error) {
	newState, err := addKeyToJson(inState, abstract.IsAsyncPartsUploadedStateKey, false)
	if err != nil {
		return "", xerrors.Errorf("unable to add key to state: %w", err)
	}
	return string(newState), nil
}

func (s *MultiWorkerTPPSetterAsync) AllPartsOrNil() []*abstract.OperationTablePart {
	return nil
}

func (s *MultiWorkerTPPSetterAsync) AsyncLoadPartsIfNeeded(
	ctx context.Context,
	storage abstract.Storage,
	tables []abstract.TableDescription,
	cp coordinator.Coordinator,
	transferID string,
	operationID string,
	checkLoaderError func() error,
) error {
	shardingContextStorage, ok := storage.(abstract.AsyncOperationPartsStorage)
	if !ok {
		return xerrors.New("storage does not implement AsyncLoadPartsIfNeeded")
	}

	err := asyncLoadParts(
		ctx,
		shardingContextStorage,
		tables,
		nil,
		cp,
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

	shardedStateBytes, err := shardingContextStorage.ShardingContext()
	if err != nil {
		return xerrors.Errorf("unable to prepare sharded state for operation '%v': %w", operationID, err)
	}
	newState, err := addKeyToJson(string(shardedStateBytes), abstract.IsAsyncPartsUploadedStateKey, true)
	if err != nil {
		return xerrors.Errorf("unable to add key to state: %w", err)
	}
	err = cp.SetOperationState(operationID, string(newState))
	if err != nil {
		return xerrors.Errorf("unable to set sharded state after upload: %w", err)
	}
	return nil
}

func NewMultiWorkerTPPSetterAsync() *MultiWorkerTPPSetterAsync {
	return &MultiWorkerTPPSetterAsync{}
}
