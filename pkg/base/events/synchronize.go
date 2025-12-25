package events

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/base"
)

type SynchronizeEvent interface {
	base.Event
	base.SupportsOldChangeItem
}

type synchronizeEvent struct {
	tbl    base.Table
	partID string
}

func (e *synchronizeEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	return &abstract.ChangeItem{
		ID:               0,
		LSN:              0,
		CommitTime:       0,
		Counter:          0,
		Kind:             abstract.SynchronizeKind,
		Schema:           e.tbl.Schema(),
		Table:            e.tbl.Name(),
		PartID:           e.partID,
		ColumnNames:      nil,
		ColumnValues:     nil,
		TableSchema:      nil,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.EmptyEventSize(),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}, nil
}

func NewDefaultSynchronizeEvent(tbl base.Table, partID string) SynchronizeEvent {
	return &synchronizeEvent{tbl, partID}
}
