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

// AssignOperationTablePartForWorker picks the first unassigned part, binds it to workerIndex
// (including main worker index 0), and returns a copy with WorkerIndex set — same idea as PG
// repository.AssignOperationTablePart.
func (p *OperationTablesParts) AssignOperationTablePartForWorker(workerIndex int) *abstract.OperationTablePart {
	p.mu.Lock()
	defer p.mu.Unlock()

	for index, table := range p.tables {
		if p.statuses[index] != tablePartStatusNew {
			continue
		}
		p.statuses[index] = tablePartStatusAssigned
		wi := workerIndex
		table.WorkerIndex = &wi
		return table.Copy()
	}
	return nil
}

// ClearAssignedTablesPartsForWorker resets in-flight assignments for workerIndex (restart path).
func (p *OperationTablesParts) ClearAssignedTablesPartsForWorker(workerIndex int) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	var cleared int64
	for index, table := range p.tables {
		if p.statuses[index] == tablePartStatusDone {
			continue
		}
		if p.statuses[index] != tablePartStatusAssigned {
			continue
		}
		if table.WorkerIndex == nil || *table.WorkerIndex != workerIndex {
			continue
		}
		p.statuses[index] = tablePartStatusNew
		table.WorkerIndex = nil
		cleared++
	}
	return cleared
}

// ListParts returns copies of all parts with Completed derived from coordinator status.
func (p *OperationTablesParts) ListParts() []*abstract.OperationTablePart {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]*abstract.OperationTablePart, 0, len(p.tables))
	for index, table := range p.tables {
		c := table.Copy()
		c.Completed = p.statuses[index] == tablePartStatusDone
		out = append(out, c)
	}
	return out
}

func operationTablePartIdentityEqual(a, b *abstract.OperationTablePart) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Schema == b.Schema && a.Name == b.Name && a.Offset == b.Offset && a.Filter == b.Filter && a.PartIndex == b.PartIndex
}

func (p *OperationTablesParts) UpdateOperationTablesParts(in *abstract.OperationTablePart) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for index, table := range p.tables {
		if !operationTablePartIdentityEqual(table, in) {
			continue
		}
		table.CompletedRows = in.CompletedRows
		table.ReadBytes = in.ReadBytes
		if in.ETARows != 0 {
			table.ETARows = in.ETARows
		}
		if in.Completed {
			p.statuses[index] = tablePartStatusDone
		}
		return nil
	}
	inSerializedBytes, _ := json.Marshal(in)
	tablesBytes, _ := json.Marshal(p.tables)
	return xerrors.Errorf("operation table part not found, in: %s, tablesBytes: %s", string(inSerializedBytes), string(tablesBytes))
}

func NewOperationTablesParts() *OperationTablesParts {
	return &OperationTablesParts{
		mu:       sync.Mutex{},
		tables:   make([]*abstract.OperationTablePart, 0),
		statuses: make([]tablePartStatus, 0),
	}
}
