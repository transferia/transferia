package events

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/base"
)

type DeleteEvent interface {
	base.SupportsOldChangeItem
	Table() base.Table
	OldValuesCount() int
	OldValue(i int) base.Value
}

type LoggedDeleteEvent interface {
	base.LoggedEvent
	DeleteEvent
}

type TransactionalDeleteEvent interface {
	base.TransactionalEvent
	LoggedDeleteEvent
}

type DefaultDeleteEvent struct {
	table     base.Table
	oldValues []base.Value
}

func NewDefaultDeleteEvent(table base.Table) *DefaultDeleteEvent {
	return &DefaultDeleteEvent{
		table:     table,
		oldValues: []base.Value{},
	}
}

func NewDefaultDeleteEventWithValues(table base.Table, oldValues []base.Value) (*DefaultDeleteEvent, error) {
	for _, oldValue := range oldValues {
		if err := validateValue(oldValue); err != nil {
			return nil, err
		}
	}
	event := &DefaultDeleteEvent{
		table:     table,
		oldValues: oldValues,
	}
	return event, nil
}

func (event *DefaultDeleteEvent) Table() base.Table {
	return event.table
}

func (event *DefaultDeleteEvent) OldValuesCount() int {
	return len(event.oldValues)
}

func (event *DefaultDeleteEvent) OldValue(i int) base.Value {
	return event.oldValues[i]
}

func (event *DefaultDeleteEvent) AddOldValue(value base.Value) error {
	if err := validateValue(value); err != nil {
		return err
	}
	event.oldValues = append(event.oldValues, value)
	return nil
}

func (event *DefaultDeleteEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	oldTable, err := event.table.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("Table cannot be converted to old format: %w", err)
	}

	changeItem := &abstract.ChangeItem{
		Kind:        abstract.DeleteKind,
		Schema:      event.table.Schema(),
		Table:       event.table.Name(),
		PartID:      "",
		TableSchema: oldTable,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{},
			KeyTypes:  []string{},
			KeyValues: []interface{}{},
		},
		ID:           0,
		LSN:          0,
		CommitTime:   0,
		Counter:      0,
		ColumnNames:  nil,
		ColumnValues: nil,
		TxID:         "",
		Query:        "",
		Size:         abstract.EmptyEventSize(),
	}

	for _, value := range event.oldValues {
		if !value.Column().Key() {
			continue
		}
		changeItem.OldKeys.KeyNames = append(changeItem.OldKeys.KeyNames, value.Column().Name())
		oldValue, err := value.ToOldValue()
		if err != nil {
			return nil, xerrors.Errorf("Value cannot be converted to old format: %w", err)
		}
		changeItem.OldKeys.KeyValues = append(changeItem.OldKeys.KeyValues, oldValue)
	}

	return changeItem, nil
}

type DefaultLoggedDeleteEvent struct {
	DefaultDeleteEvent
	position base.LogPosition
}

func NewDefaultLoggedDeleteEvent(
	table base.Table,
	position base.LogPosition,
) *DefaultLoggedDeleteEvent {
	return &DefaultLoggedDeleteEvent{
		DefaultDeleteEvent: *NewDefaultDeleteEvent(table),
		position:           position,
	}
}

func NewDefaultLoggedDeleteEventWithValues(
	table base.Table,
	position base.LogPosition,
	oldValues []base.Value,
) (*DefaultLoggedDeleteEvent, error) {
	deleteEvent, err := NewDefaultDeleteEventWithValues(table, oldValues)
	if err != nil {
		return nil, err
	}
	loggedDeleteEvent := &DefaultLoggedDeleteEvent{
		DefaultDeleteEvent: *deleteEvent,
		position:           position,
	}
	return loggedDeleteEvent, nil
}

func (event *DefaultLoggedDeleteEvent) Position() base.LogPosition {
	return event.position
}

func (event *DefaultLoggedDeleteEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	changeItem, err := event.DefaultDeleteEvent.ToOldChangeItem()
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

type DefaultTransactionalDeleteEvent struct {
	DefaultLoggedDeleteEvent
	transaction base.Transaction
}

func NewDefaultTransactionalDeleteEvent(
	table base.Table,
	position base.LogPosition,
	transaction base.Transaction,
) *DefaultTransactionalDeleteEvent {
	return &DefaultTransactionalDeleteEvent{
		DefaultLoggedDeleteEvent: *NewDefaultLoggedDeleteEvent(table, position),
		transaction:              transaction,
	}
}

func NewDefaultTransactionalDeleteEventWithValues(
	table base.Table,
	position base.LogPosition,
	transaction base.Transaction,
	oldValues []base.Value,
) (*DefaultTransactionalDeleteEvent, error) {
	loggedDeleteEvent, err := NewDefaultLoggedDeleteEventWithValues(table, position, oldValues)
	if err != nil {
		return nil, err
	}
	transactionalDeleteEvent := &DefaultTransactionalDeleteEvent{
		DefaultLoggedDeleteEvent: *loggedDeleteEvent,
		transaction:              transaction,
	}
	return transactionalDeleteEvent, nil
}

func (event *DefaultTransactionalDeleteEvent) Transaction() base.Transaction {
	return event.transaction
}

func (event *DefaultTransactionalDeleteEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	if changeItem, err := event.DefaultLoggedDeleteEvent.ToOldChangeItem(); err != nil {
		return nil, err
	} else {
		changeItem.TxID = event.Transaction().String()
		return changeItem, nil
	}
}
