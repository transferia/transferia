package events

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
)

type TableLoadState int

// It is important for serialization not to use iota
const (
	InitShardedTableLoad = TableLoadState(4)
	TableLoadBegin       = TableLoadState(1)
	TableLoadEnd         = TableLoadState(2)
	DoneShardedTableLoad = TableLoadState(3)
)

func (s TableLoadState) String() string {
	switch s {
	case InitShardedTableLoad:
		return "InitShardedTableLoad"
	case TableLoadBegin:
		return "TableLoadBegin"
	case TableLoadEnd:
		return "TableLoadEnd"
	case DoneShardedTableLoad:
		return "DoneShardedTableLoad"
	default:
		return fmt.Sprintf("Unknown event %d", int(s))
	}
}

type TableLoadEvent interface {
	base.Event
	base.SupportsOldChangeItem
	Table() base.Table
	State() TableLoadState
}

type DefaultTableLoadEvent struct {
	table base.Table
	state TableLoadState
	part  string
}

func NewDefaultTableLoadEvent(table base.Table, state TableLoadState) *DefaultTableLoadEvent {
	return &DefaultTableLoadEvent{
		table: table,
		state: state,
		part:  "",
	}
}

func (event *DefaultTableLoadEvent) String() string {
	return event.state.String()
}

func (event *DefaultTableLoadEvent) Table() base.Table {
	return event.table
}

func (event *DefaultTableLoadEvent) State() TableLoadState {
	return event.state
}

func (event *DefaultTableLoadEvent) WithPart(part string) *DefaultTableLoadEvent {
	event.part = part
	return event
}

func (event *DefaultTableLoadEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	var kind abstract.Kind
	switch event.State() {
	case InitShardedTableLoad:
		kind = abstract.InitShardedTableLoad
	case TableLoadBegin:
		kind = abstract.InitTableLoad
	case TableLoadEnd:
		kind = abstract.DoneTableLoad
	case DoneShardedTableLoad:
		kind = abstract.DoneShardedTableLoad
	default:
		return nil, xerrors.Errorf("Invalid state '%v'", event.State())
	}

	schema, err := event.table.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("error getting old table schema: %w", err)
	}

	return &abstract.ChangeItem{
		Kind:         kind,
		Schema:       event.table.Schema(),
		Table:        event.table.Name(),
		PartID:       event.part,
		ID:           0,
		LSN:          0,
		CommitTime:   0,
		Counter:      0,
		ColumnNames:  nil,
		ColumnValues: nil,
		TableSchema:  schema,
		OldKeys: abstract.OldKeysType{
			KeyNames:  nil,
			KeyTypes:  nil,
			KeyValues: nil,
		},
		TxID:  "",
		Query: "",
		Size:  abstract.EmptyEventSize(),
	}, nil
}
