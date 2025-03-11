package events

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/util"
)

type insertBuilder struct {
	table       base.Table
	vals        []base.Value
	errs        util.Errors
	position    base.LogPosition
	transaction base.Transaction
	keyPos      map[string]int
}

func (b *insertBuilder) FromMap(record map[string]interface{}) InsertBuilder {
	for i := 0; i < b.table.ColumnsCount(); i++ {
		col := b.table.Column(i)
		if v, ok := record[col.FullName()]; ok {
			b.Column(col.FullName(), v)
		} else {
			b.Column(col.FullName(), nil)
		}
	}
	return b
}

func (b *insertBuilder) Build() (InsertEvent, error) {
	var err error
	for i, v := range b.vals {
		if v == nil {
			b.vals[i], err = b.table.Column(i).Value(nil)
			if err != nil {
				b.errs = append(b.errs, err)
			}
		}
	}
	if len(b.errs) > 0 {
		return nil, xerrors.Errorf("unable to build event: %w", b.errs)
	}
	return b.newEvent()
}

func (b *insertBuilder) newEvent() (InsertEvent, error) {
	if b.position != nil && b.transaction != nil {
		iev, err := NewDefaultTransactionalInsertEventWithValues(b.table, b.position, b.transaction, b.vals)
		if err != nil {
			return nil, xerrors.Errorf("unable to create transactional insert: %w", err)
		}
		return iev, err
	}
	iev, err := NewDefaultInsertEventWithValues(b.table, b.vals)
	if err != nil {
		return nil, xerrors.Errorf("unable to create event: %w", err)
	}
	return iev, nil
}

func (b *insertBuilder) Column(colName string, val interface{}) InsertBuilder {
	col := b.table.ColumnByName(colName)
	if col == nil {
		b.errs = append(b.errs, xerrors.Errorf("column: %v not exist in schema", colName))
		return b
	}
	if val == nil {
		if col.Nullable() {
			v, err := col.Value(nil)
			if err != nil {
				b.errs = append(b.errs, err)
				return b
			}
			b.vals[b.keyPos[col.FullName()]] = v
			return b
		}
		b.errs = append(b.errs, xerrors.Errorf("unable to add nil value to no nullable column: %v", colName))
		return b
	}
	v, err := col.Value(val)
	if err != nil {
		b.errs = append(b.errs, err)
		return b
	}
	b.vals[b.keyPos[col.FullName()]] = v
	return b
}

type InsertBuilder interface {
	Column(colName string, val interface{}) InsertBuilder
	Build() (InsertEvent, error)
	FromMap(record map[string]interface{}) InsertBuilder
}

func NewDefaultInsertBuilder(table base.Table) InsertBuilder {
	keyPos := map[string]int{}
	for i := 0; i < table.ColumnsCount(); i++ {
		keyPos[table.Column(i).FullName()] = i
	}
	return &insertBuilder{
		table:       table,
		keyPos:      keyPos,
		vals:        make([]base.Value, table.ColumnsCount()),
		errs:        nil,
		position:    nil,
		transaction: nil,
	}
}
