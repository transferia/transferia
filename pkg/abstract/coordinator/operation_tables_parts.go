package coordinator

import (
	"encoding/json"
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

func clearUseless(in *abstract.OperationTablePart) *abstract.OperationTablePart {
	inCopy := in.Copy()
	inCopy.CompletedRows = 0
	inCopy.ReadBytes = 0
	inCopy.Completed = false
	return inCopy
}

func (p *OperationTablesParts) CreateOperationTablesParts(inTables []*abstract.OperationTablePart) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, table := range inTables {
		p.tables = append(p.tables, clearUseless(table))
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
			return table.Copy()
		}
	}
	return nil
}

func (p *OperationTablesParts) UpdateOperationTablesParts(in *abstract.OperationTablePart) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	inCleared := clearUseless(in)

	for index, table := range p.tables {
		if *table == *inCleared {
			if in.Completed {
				p.statuses[index] = tablePartStatusDone
			}
			return nil
		}
	}
	inClearedSerializedBytes, _ := json.Marshal(inCleared)
	tablesBytes, _ := json.Marshal(p.tables)
	return xerrors.Errorf("operation table part not found, inCleared: %s, tablesBytes: %s", string(inClearedSerializedBytes), string(tablesBytes))
}

func NewOperationTablesParts() *OperationTablesParts {
	return &OperationTablesParts{
		mu:       sync.Mutex{},
		tables:   make([]*abstract.OperationTablePart, 0),
		statuses: make([]tablePartStatus, 0),
	}
}
