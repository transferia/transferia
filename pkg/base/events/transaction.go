package events

import (
	"github.com/transferria/transferria/pkg/base"
)

type TransactionState int

// It is important for serialization not to use iota
const (
	TransactionBegin  TransactionState = TransactionState(1)
	TransactionCommit TransactionState = TransactionState(2)
)

type TransactionEvent interface {
	base.TransactionalEvent
	State() TransactionState
}

type DefaultTransactionEvent struct {
	position    base.LogPosition
	transaction base.Transaction
	state       TransactionState
}

func NewDefaultTransactionEvent(position base.LogPosition, transaction base.Transaction, state TransactionState) *DefaultTransactionEvent {
	return &DefaultTransactionEvent{
		position:    position,
		transaction: transaction,
		state:       state,
	}
}

func (event *DefaultTransactionEvent) Position() base.LogPosition {
	return event.position
}

func (event *DefaultTransactionEvent) Transaction() base.Transaction {
	return event.transaction
}

func (event *DefaultTransactionEvent) State() TransactionState {
	return event.state
}
