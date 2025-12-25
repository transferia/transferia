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
	parts          []*abstract.OperationTablePart
	operationState map[string]string
}

func (m *Local) Store(in []*abstract.OperationTablePart) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.parts = append(m.parts, in...)
	return nil
}

func (m *Local) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.parts) == 0 {
		return nil, nil
	}
	result := m.parts[0]
	m.parts = m.parts[1:]
	return result, nil
}

func (m *Local) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	return nil
}

func (m *Local) Close() error {
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
		parts:          nil,
		operationState: make(map[string]string),
	}
}
