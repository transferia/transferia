package shared_memory

import (
	"context"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

// To verify providers contract implementation
var (
	_ abstract.SharedMemory = (*Remote)(nil)
)

type Remote struct {
	mu          sync.Mutex
	cp          coordinator.Coordinator
	operationID string
	workerIndex int
}

func (m *Remote) Store(in []*abstract.OperationTablePart) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.cp.CreateOperationTablesParts(m.operationID, in)
	if err != nil {
		return xerrors.Errorf("Failed to CreateOperationTablesParts: %w", err)
	}
	return nil
}

func (m *Remote) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	return m.cp.AssignOperationTablePart(m.operationID, m.workerIndex)
}

func (m *Remote) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	return m.cp.UpdateOperationTablesParts(operationID, tables)
}

func (m *Remote) Close() error {
	return nil
}

func (m *Remote) GetShardStateNoWait(ctx context.Context, operationID string) (string, error) {
	return GetShardStateNoWait(ctx, m.cp, operationID)
}

func (m *Remote) SetOperationState(operationID string, newState string) error {
	return m.cp.SetOperationState(operationID, newState)
}

func NewRemote(cp coordinator.Coordinator, operationID string, workerIndex int) *Remote {
	return &Remote{
		mu:          sync.Mutex{},
		cp:          cp,
		operationID: operationID,
		workerIndex: workerIndex,
	}
}
