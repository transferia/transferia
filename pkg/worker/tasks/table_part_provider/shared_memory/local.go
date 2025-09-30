package shared_memory

import (
	"context"
	"sync"

	"github.com/transferia/transferia/pkg/abstract"
)

// To verify providers contract implementation
var (
	_ abstract.SharedMemory = (*Local)(nil)
)

type Local struct {
	mu             sync.Mutex
	operationID    string
	allParts       []abstract.TableDescription
	currParts      []abstract.TableDescription
	operationState map[string]string
}

func (m *Local) ResetState() error {
	return nil // no-op
}

func (m *Local) Store(in []abstract.TableDescription) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.allParts = append(m.allParts, in...)
	m.currParts = append(m.currParts, in...)
	return nil
}

func (m *Local) ConvertToTableDescription(in *abstract.OperationTablePart) (*abstract.TableDescription, error) {
	return in.ToTableDescription(), nil
}

func (m *Local) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.currParts) == 0 {
		return nil, nil
	}
	result := m.currParts[0]
	m.currParts = m.currParts[1:]
	return abstract.NewOperationTablePartFromDescription(m.operationID, &result), nil
}

func (m *Local) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	return nil
}

func (m *Local) RemoveTransferState(transferID string, stateKeys []string) error {
	return nil
}

func (m *Local) GetShardStateNoWait(_ context.Context, operationID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.operationState[operationID], nil
}

func (m *Local) SetOperationState(operationID string, newState string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.operationState[operationID] = newState
	return nil
}

func NewLocal(operationID string) *Local {
	return &Local{
		mu:             sync.Mutex{},
		operationID:    operationID,
		allParts:       nil,
		currParts:      nil,
		operationState: make(map[string]string),
	}
}
