package events

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/base"
)

type UpdateEvent interface {
	base.SupportsOldChangeItem
	Table() base.Table
	NewValuesCount() int
	NewValue(i int) base.Value
	OldValuesCount() int
	OldValue(i int) base.Value
}

type LoggedUpdateEvent interface {
	base.LoggedEvent
	UpdateEvent
}

type TransactionalUpdateEvent interface {
	base.TransactionalEvent
	LoggedUpdateEvent
}

type DefaultUpdateEvent struct {
	table     base.Table
	newValues []base.Value
	oldValues []base.Value
}

func NewDefaultUpdateEvent(table base.Table) *DefaultUpdateEvent {
	return &DefaultUpdateEvent{
		table:     table,
		newValues: []base.Value{},
		oldValues: []base.Value{},
	}
}

func NewDefaultUpdateEventWithValues(table base.Table, newValues []base.Value, oldValues []base.Value) (*DefaultUpdateEvent, error) {
	for _, newValue := range newValues {
		if err := validateValue(newValue); err != nil {
			return nil, xerrors.Errorf("old value is invalid: %w", err)
		}
	}
	for _, oldValue := range oldValues {
		if err := validateValue(oldValue); err != nil {
			return nil, xerrors.Errorf("new value is invalid: %w", err)
		}
	}
	event := &DefaultUpdateEvent{
		table:     table,
		newValues: newValues,
		oldValues: oldValues,
	}
	return event, nil
}

func (event *DefaultUpdateEvent) Table() base.Table {
	return event.table
}

func (event *DefaultUpdateEvent) NewValuesCount() int {
	return len(event.newValues)
}

func (event *DefaultUpdateEvent) NewValue(i int) base.Value {
	return event.newValues[i]
}

func (event *DefaultUpdateEvent) AddNewValue(value base.Value) error {
	if err := validateValue(value); err != nil {
		return err
	}
	event.newValues = append(event.newValues, value)
	return nil
}

func (event *DefaultUpdateEvent) OldValuesCount() int {
	return len(event.oldValues)
}

func (event *DefaultUpdateEvent) OldValue(i int) base.Value {
	return event.oldValues[i]
}

func (event *DefaultUpdateEvent) AddOldValue(value base.Value) error {
	if err := validateValue(value); err != nil {
		return err
	}
	event.oldValues = append(event.oldValues, value)
	return nil
}

func (event *DefaultUpdateEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	oldTable, err := event.table.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("Table cannot be converted to old format: %w", err)
	}

	changeItem := &abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   0,
		Counter:      0,
		Kind:         abstract.UpdateKind,
		Schema:       event.table.Schema(),
		Table:        event.table.Name(),
		PartID:       "",
		ColumnNames:  []string{},
		ColumnValues: []interface{}{},
		TableSchema:  oldTable,
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{},
			KeyTypes:  []string{},
			KeyValues: []interface{}{},
		},
		Size:             abstract.EmptyEventSize(),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}

	for _, value := range event.newValues {
		changeItem.ColumnNames = append(changeItem.ColumnNames, value.Column().Name())
		oldValue, err := value.ToOldValue()
		if err != nil {
			return nil, xerrors.Errorf("Value cannot be converted to old format: %w", err)
		}
		changeItem.ColumnValues = append(changeItem.ColumnValues, oldValue)
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

type DefaultLoggedUpdateEvent struct {
	DefaultUpdateEvent
	position base.LogPosition
}

func NewDefaultLoggedUpdateEvent(
	table base.Table,
	position base.LogPosition,
) *DefaultLoggedUpdateEvent {
	return &DefaultLoggedUpdateEvent{
		DefaultUpdateEvent: *NewDefaultUpdateEvent(table),
		position:           position,
	}
}

func NewDefaultLoggedUpdateEventWithValues(
	table base.Table,
	position base.LogPosition,
	newValues []base.Value,
	oldValues []base.Value,
) (*DefaultLoggedUpdateEvent, error) {
	updateEvent, err := NewDefaultUpdateEventWithValues(table, newValues, oldValues)
	if err != nil {
		return nil, err
	}
	loggedUpdateEvent := &DefaultLoggedUpdateEvent{
		DefaultUpdateEvent: *updateEvent,
		position:           position,
	}
	return loggedUpdateEvent, nil
}

func (event *DefaultLoggedUpdateEvent) Position() base.LogPosition {
	return event.position
}

func (event *DefaultLoggedUpdateEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	changeItem, err := event.DefaultUpdateEvent.ToOldChangeItem()
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

type DefaultTransactionalUpdateEvent struct {
	DefaultLoggedUpdateEvent
	transaction base.Transaction
}

func NewDefaultTransactionalUpdateEvent(
	table base.Table,
	position base.LogPosition,
	transaction base.Transaction,
) *DefaultTransactionalUpdateEvent {
	return &DefaultTransactionalUpdateEvent{
		DefaultLoggedUpdateEvent: *NewDefaultLoggedUpdateEvent(table, position),
		transaction:              transaction,
	}
}

func NewDefaultTransactionalUpdateEventWithValues(
	table base.Table,
	position base.LogPosition,
	transaction base.Transaction,
	newValues []base.Value,
	oldValues []base.Value,
) (*DefaultTransactionalUpdateEvent, error) {
	loggedUpdateEvent, err := NewDefaultLoggedUpdateEventWithValues(table, position, newValues, oldValues)
	if err != nil {
		return nil, err
	}
	transactionalUpdateEvent := &DefaultTransactionalUpdateEvent{
		DefaultLoggedUpdateEvent: *loggedUpdateEvent,
		transaction:              transaction,
	}
	return transactionalUpdateEvent, nil
}

func (event *DefaultTransactionalUpdateEvent) Transaction() base.Transaction {
	return event.transaction
}

func (event *DefaultTransactionalUpdateEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	if changeItem, err := event.DefaultLoggedUpdateEvent.ToOldChangeItem(); err != nil {
		return nil, err
	} else {
		changeItem.TxID = event.Transaction().String()
		return changeItem, nil
	}
}
