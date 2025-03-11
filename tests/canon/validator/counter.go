package validator

import (
	"context"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type Counter struct {
	mu           *sync.RWMutex
	rowsInTable  map[string]uint64
	tableSchemas map[string]*abstract.TableSchema
}

func (c *Counter) Close() {
}

func (c *Counter) Ping() error {
	return nil
}

func (c *Counter) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	return xerrors.New("Not implemented")
}

func (c *Counter) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return c.getSchema(table)
}

func (c *Counter) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	return nil, xerrors.New("Not implemented")
}

func (c *Counter) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return c.getCount(table)
}

func (c *Counter) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return c.getCount(table)
}

func (c *Counter) TableExists(table abstract.TableID) (bool, error) {
	_, err := c.getCount(table)
	return err == nil, nil
}

func (c *Counter) add(tableID abstract.TableID, schema *abstract.TableSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rowsInTable[tableID.Fqtn()] += 1
	c.tableSchemas[tableID.Fqtn()] = schema
}

func (c *Counter) delete(tableID abstract.TableID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rowsInTable[tableID.Fqtn()] -= 1
}

func (c *Counter) drop(tableID abstract.TableID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.rowsInTable, tableID.Fqtn())
	delete(c.tableSchemas, tableID.Fqtn())
}

func (c *Counter) Truncate(tableID abstract.TableID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rowsInTable[tableID.Fqtn()] = 0
}

func (c *Counter) getSchema(tableID abstract.TableID) (*abstract.TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if schema, ok := c.tableSchemas[tableID.Fqtn()]; ok {
		return schema, nil
	}
	return nil, xerrors.Errorf("table %q not found", tableID.Fqtn())
}

func (c *Counter) getCount(tableID abstract.TableID) (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if rows, ok := c.rowsInTable[tableID.Fqtn()]; ok {
		return rows, nil
	}
	return 0, xerrors.Errorf("table %q not found", tableID.Fqtn())
}

func (c *Counter) GetSum() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var sum uint64
	for _, u := range c.rowsInTable {
		sum += u
	}
	return sum
}

type CounterSink struct {
	counter *Counter
}

func (c *CounterSink) Close() error {
	return nil
}

func (c *CounterSink) Push(items []abstract.ChangeItem) error {
	for _, row := range items {
		if row.IsSystemTable() {
			continue
		}
		switch row.Kind {
		case abstract.InsertKind:
			c.counter.add(row.TableID(), row.TableSchema)
		case abstract.DeleteKind:
			c.counter.delete(row.TableID())
		case abstract.DropTableKind:
			c.counter.drop(row.TableID())
		case abstract.TruncateTableKind:
			c.counter.Truncate(row.TableID())
		default:
			continue
		}
	}
	return nil
}

func NewCounter() (*Counter, func() abstract.Sinker) {
	storage := &Counter{
		mu:           &sync.RWMutex{},
		rowsInTable:  map[string]uint64{},
		tableSchemas: map[string]*abstract.TableSchema{},
	}
	return storage, func() abstract.Sinker {
		return &CounterSink{counter: storage}
	}
}
