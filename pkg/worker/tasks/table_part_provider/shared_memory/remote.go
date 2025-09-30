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

func (m *Remote) ResetState() error {
	return nil // no-op
}

func (m *Remote) Store(in []abstract.TableDescription) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	arrTablePart := abstract.NewOperationTablePartFromDescriptionArr(m.operationID, in...)
	err := m.cp.CreateOperationTablesParts(m.operationID, arrTablePart)
	if err != nil {
		return xerrors.Errorf("Failed to CreateOperationTablesParts: %w", err)
	}
	return nil
}

func (m *Remote) ConvertToTableDescription(in *abstract.OperationTablePart) (*abstract.TableDescription, error) {
	return in.ToTableDescription(), nil
}

func (m *Remote) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	return m.cp.AssignOperationTablePart(m.operationID, m.workerIndex)
}

func (m *Remote) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	return m.cp.UpdateOperationTablesParts(operationID, tables)
}

func (m *Remote) RemoveTransferState(transferID string, stateKeys []string) error {
	return m.cp.RemoveTransferState(transferID, stateKeys)
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
