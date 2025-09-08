package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

type MultiWorkerTPPGetterSync struct {
	cp          coordinator.Coordinator
	operationID string
	workerIndex int
}

func (g *MultiWorkerTPPGetterSync) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	return g.cp.AssignOperationTablePart(g.operationID, g.workerIndex)
}

func (g *MultiWorkerTPPGetterSync) AllPartsOrNil() []*abstract.OperationTablePart {
	return nil
}

func NewMultiWorkerTPPGetterSync(
	cp coordinator.Coordinator,
	operationID string,
	workerIndex int,
) AbstractTablePartProviderGetter {
	return &MultiWorkerTPPGetterSync{
		cp:          cp,
		operationID: operationID,
		workerIndex: workerIndex,
	}
}
