package provider

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract2"
)

type event struct {
	parentBatch *batch
	idx         int
	rawSize     uint64
}

func (e *event) ToOldChangeItem() (*abstract.ChangeItem, error) {
	oldTable, err := e.parentBatch.table.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("table cannot be converted to old format: %w", err)
	}
	colNames, err := e.parentBatch.table.ColumnNames()
	if err != nil {
		return nil, xerrors.Errorf("error getting column names: %w", err)
	}

	row := e.parentBatch.rows[e.idx]
	return &abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   0,
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       e.parentBatch.table.Schema(),
		Table:        e.parentBatch.table.Name(),
		PartID:       e.parentBatch.part,
		ColumnNames:  colNames,
		ColumnValues: row.values,
		TableSchema:  oldTable,
		OldKeys: abstract.OldKeysType{
			KeyNames:  nil,
			KeyTypes:  nil,
			KeyValues: nil,
		},
		Size:             abstract.RawEventSize(e.rawSize),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}, nil
}

func (e *event) Table() abstract2.Table {
	return e.parentBatch.table
}

func NewEventFromDecodedRow(parentBatch *batch, idx int) *event {
	return &event{
		parentBatch: parentBatch,
		idx:         idx,
		rawSize:     uint64(parentBatch.rows[idx].rawSize),
	}
}
