package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

type MultiWorkerTPPSetterSync struct {
	allParts []*abstract.OperationTablePart
}

func (s *MultiWorkerTPPSetterSync) AppendParts(ctx context.Context, parts []*abstract.OperationTablePart) error {
	s.allParts = append(s.allParts, parts...)
	return nil
}

func (s *MultiWorkerTPPSetterSync) EnrichShardedState(state string) (string, error) {
	return state, nil
}

func (s *MultiWorkerTPPSetterSync) AllPartsOrNil() []*abstract.OperationTablePart {
	return s.allParts
}

func (s *MultiWorkerTPPSetterSync) AsyncLoadPartsIfNeeded(
	ctx context.Context,
	storage abstract.Storage,
	tables []abstract.TableDescription,
	cp coordinator.Coordinator,
	transferID string,
	operationID string,
	checkLoaderError func() error,
) error {
	return nil
}

func NewMultiWorkerTPPSetterSync() *MultiWorkerTPPSetterSync {
	return &MultiWorkerTPPSetterSync{
		allParts: nil,
	}
}
