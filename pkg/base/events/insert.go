package events

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
)

type InsertEvent interface {
	base.SupportsOldChangeItem
	Table() base.Table
	NewValuesCount() int
	NewValue(i int) (base.Value, error)
}

type LoggedInsertEvent interface {
	base.LoggedEvent
	InsertEvent
}

type TransactionalInsertEvent interface {
	base.TransactionalEvent
	LoggedInsertEvent
}

type DefaultInsertEvent struct {
	table     base.Table
	newValues []base.Value
}

func NewDefaultInsertEvent(table base.Table) *DefaultInsertEvent {
	return &DefaultInsertEvent{
		table:     table,
		newValues: []base.Value{},
	}
}

func NewDefaultInsertEventWithValues(table base.Table, newValues []base.Value) (*DefaultInsertEvent, error) {
	for _, newValue := range newValues {
		if err := validateValue(newValue); err != nil {
			return nil, err
		}
	}
	event := &DefaultInsertEvent{
		table:     table,
		newValues: newValues,
	}
	return event, nil
}

func (event *DefaultInsertEvent) Table() base.Table {
	return event.table
}

func (event *DefaultInsertEvent) NewValuesCount() int {
	return len(event.newValues)
}

func (event *DefaultInsertEvent) NewValue(i int) (base.Value, error) {
	return event.newValues[i], nil
}

func (event *DefaultInsertEvent) AddNewValue(value base.Value) error {
	if err := validateValue(value); err != nil {
		return err
	}
	event.newValues = append(event.newValues, value)
	return nil
}

func (event *DefaultInsertEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	oldTable, err := event.table.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("Table cannot be converted to old format: %w", err)
	}

	valCnt := len(event.newValues)
	changeItem := &abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		Schema:       event.table.Schema(),
		Table:        event.table.Name(),
		PartID:       "",
		TableSchema:  oldTable,
		ColumnNames:  make([]string, valCnt),
		ColumnValues: make([]interface{}, valCnt),
		ID:           0,
		LSN:          0,
		CommitTime:   0,
		Counter:      0,
		OldKeys: abstract.OldKeysType{
			KeyNames:  nil,
			KeyTypes:  nil,
			KeyValues: nil,
		},
		TxID:  "",
		Query: "",
		Size:  abstract.EmptyEventSize(),
	}

	for idx, value := range event.newValues {
		changeItem.ColumnNames[idx] = value.Column().Name()
		oldValue, err := value.ToOldValue()
		if err != nil {
			return nil, xerrors.Errorf("Value cannot be converted to old format: %w", err)
		}
		changeItem.ColumnValues[idx] = oldValue
	}

	return changeItem, nil
}

type DefaultLoggedInsertEvent struct {
	DefaultInsertEvent
	position base.LogPosition
}

func NewDefaultLoggedInsertEvent(
	table base.Table,
	position base.LogPosition,
) *DefaultLoggedInsertEvent {
	return &DefaultLoggedInsertEvent{
		DefaultInsertEvent: *NewDefaultInsertEvent(table),
		position:           position,
	}
}

func NewDefaultLoggedInsertEventWithValues(
	table base.Table,
	position base.LogPosition,
	newValues []base.Value,
) (*DefaultLoggedInsertEvent, error) {
	insertEvent, err := NewDefaultInsertEventWithValues(table, newValues)
	if err != nil {
		return nil, err
	}
	loggedInsertEvent := &DefaultLoggedInsertEvent{
		DefaultInsertEvent: *insertEvent,
		position:           position,
	}
	return loggedInsertEvent, nil
}

func (event *DefaultLoggedInsertEvent) Position() base.LogPosition {
	return event.position
}

func (event *DefaultLoggedInsertEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	changeItem, err := event.DefaultInsertEvent.ToOldChangeItem()
	if err != nil {
		return nil, xerrors.Errorf("Can't convert event to legacy change item: %w", err)
	}
	if supportsLegacyLSN, ok := event.Position().(base.SupportsOldLSN); ok {
		lsn, err := supportsLegacyLSN.ToOldLSN()
		if err != nil {
			return nil, xerrors.Errorf("Can't get legacy LSN from log position: %w", err)
		}
		changeItem.LSN = lsn
	}
	if supportsLegacyCommitTime, ok := event.Position().(base.SupportsOldCommitTime); ok {
		commitTime, err := supportsLegacyCommitTime.ToOldCommitTime()
		if err != nil {
			return nil, xerrors.Errorf("Can't get legacy LSN from log position: %w", err)
		}
		changeItem.CommitTime = commitTime
	}
	return changeItem, nil
}

type DefaultTransactionalInsertEvent struct {
	DefaultLoggedInsertEvent
	transaction base.Transaction
}

func NewDefaultTransactionalInsertEvent(
	table base.Table,
	position base.LogPosition,
	transaction base.Transaction,
) *DefaultTransactionalInsertEvent {
	return &DefaultTransactionalInsertEvent{
		DefaultLoggedInsertEvent: *NewDefaultLoggedInsertEvent(table, position),
		transaction:              transaction,
	}
}

func NewDefaultTransactionalInsertEventWithValues(
	table base.Table,
	position base.LogPosition,
	transaction base.Transaction,
	newValues []base.Value,
) (*DefaultTransactionalInsertEvent, error) {
	loggedInsertEvent, err := NewDefaultLoggedInsertEventWithValues(table, position, newValues)
	if err != nil {
		return nil, err
	}
	transactionalInsertEvent := &DefaultTransactionalInsertEvent{
		DefaultLoggedInsertEvent: *loggedInsertEvent,
		transaction:              transaction,
	}
	return transactionalInsertEvent, nil
}

func (event *DefaultTransactionalInsertEvent) Transaction() base.Transaction {
	return event.transaction
}

func (event *DefaultTransactionalInsertEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	if changeItem, err := event.DefaultLoggedInsertEvent.ToOldChangeItem(); err != nil {
		return nil, err
	} else {
		changeItem.TxID = event.Transaction().String()
		return changeItem, nil
	}
}
