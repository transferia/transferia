package coordinator

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type tablePartStatus int

const (
	tablePartStatusNew = iota
	tablePartStatusAssigned
	tablePartStatusDone
)

type OperationTablesParts struct {
	mu sync.Mutex

	tables   []*abstract.OperationTablePart
	statuses []tablePartStatus
}

func (p *OperationTablesParts) CreateOperationTablesParts(inTables []*abstract.OperationTablePart) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, table := range inTables {
		p.tables = append(p.tables, table)
		p.statuses = append(p.statuses, tablePartStatusNew)
	}
	return nil
}

func (p *OperationTablesParts) AssignOperationTablePart() *abstract.OperationTablePart {
	p.mu.Lock()
	defer p.mu.Unlock()

	for index, table := range p.tables {
		if p.statuses[index] == tablePartStatusNew {
			p.statuses[index] = tablePartStatusAssigned
			return table
		}
	}
	return nil
}

func (p *OperationTablesParts) UpdateOperationTablesParts(in *abstract.OperationTablePart) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for index, table := range p.tables {
		if table == in {
			p.statuses[index] = tablePartStatusDone
			return nil
		}
	}
	return xerrors.New("operation table part not found")
}

func NewOperationTablesParts() *OperationTablesParts {
	return &OperationTablesParts{
		mu:       sync.Mutex{},
		tables:   make([]*abstract.OperationTablePart, 0),
		statuses: make([]tablePartStatus, 0),
	}
}
