//go:build !disable_yt_provider

package provider

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers/yt/provider/types"
	"go.ytsaurus.tech/yt/go/yson"
)

type event struct {
	parentBatch *batch
	idx         int
	rawSize     uint64
	row         map[string]interface{}
	mutex       sync.Mutex
}

func (e *event) maybeUnmarshal() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.row != nil {
		return nil
	}
	data := make(map[string]any, e.parentBatch.table.ColumnsCount())
	lazyRow := e.parentBatch.rows[e.idx]
	if err := yson.Unmarshal(lazyRow.data, &data); err != nil {
		return xerrors.Errorf("unable to marshal: %w", err)
	}
	if e.parentBatch.idxCol != "" {
		data[e.parentBatch.idxCol] = lazyRow.rowIDX
	}
	e.row = data
	return nil
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

	cnt := e.parentBatch.table.ColumnsCount()
	changeItem := &abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		Schema:       e.parentBatch.table.Schema(),
		Table:        e.parentBatch.table.Name(),
		PartID:       e.parentBatch.part,
		TableSchema:  oldTable,
		ColumnNames:  colNames,
		ColumnValues: make([]interface{}, cnt),
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
		Size:  abstract.RawEventSize(e.rawSize),
	}

	for i := 0; i < cnt; i++ {
		val, err := e.NewValue(i)
		if err != nil {
			return nil, xerrors.Errorf("error getting row value %d: %w", i, err)
		}
		oldVal, err := val.ToOldValue()
		if err != nil {
			return nil, xerrors.Errorf("error converting row value %d to old format: %w", i, err)
		}
		changeItem.ColumnValues[i] = oldVal
	}
	return changeItem, nil
}

func (e *event) Table() base.Table {
	return e.parentBatch.table
}

func (e *event) NewValuesCount() int {
	return e.parentBatch.table.ColumnsCount()
}

func (e *event) NewValue(i int) (base.Value, error) {
	if err := e.maybeUnmarshal(); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal: %w", err)
	}
	col := e.parentBatch.table.Column(i)
	if col == nil {
		return nil, xerrors.Errorf("unknown column %d", i)
	}
	raw, ok := e.row[col.Name()]
	if !ok {
		return nil, xerrors.Errorf("expected column %s to be present in row from YT", col.Name())
	}
	val, err := types.Cast(raw, col)
	if err != nil {
		return nil, xerrors.Errorf("unable to cast column %s with raw type %T to type system: %w", col.Name(), raw, err)
	}
	return val, nil
}

func NewEventFromLazyYSON(parentBatch *batch, idx int) *event {
	return &event{
		parentBatch: parentBatch,
		idx:         idx,
		rawSize:     uint64(parentBatch.rows[idx].RawSize()),
		row:         nil,
		mutex:       sync.Mutex{},
	}
}
