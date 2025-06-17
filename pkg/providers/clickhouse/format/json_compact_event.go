//go:build !disable_clickhouse_provider

package format

import (
	"encoding/json"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/adapter"
	"github.com/transferia/transferia/pkg/base/events"
)

type JSONCompactEvent struct {
	row        []byte
	cols       *abstract.TableSchema
	table      abstract.TableID
	colNames   []string
	readerTime time.Time
}

func (e *JSONCompactEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	// this method for debug only purpose, we don't have good csv parser yet
	var vals []interface{}
	if err := json.Unmarshal(e.row, &vals); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal row: %w", err)
	}
	row := make([]interface{}, len(e.colNames))
	for i, col := range e.cols.Columns() {
		if len(vals) <= i {
			continue
		}
		row[i] = abstract.Restore(col, vals[i])
	}
	return &abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   uint64(e.readerTime.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       e.table.Namespace,
		Table:        e.table.Name,
		PartID:       "",
		ColumnNames:  e.colNames,
		ColumnValues: row,
		TableSchema:  e.cols,
		OldKeys:      *new(abstract.OldKeysType),
		TxID:         "",
		Query:        "",
		Size:         abstract.EmptyEventSize(),
	}, nil
}

func (e *JSONCompactEvent) Table() base.Table {
	return adapter.NewTableFromLegacy(e.cols, e.table)
}

func (e *JSONCompactEvent) NewValuesCount() int {
	return len(e.cols.Columns())
}

func (e *JSONCompactEvent) NewValue(i int) (base.Value, error) {
	return nil, xerrors.New("compact json event NewValue not implemented")
}

func NewJSONCompactEvent(row []byte, cols *abstract.TableSchema, names []string, table abstract.TableID, readerTime time.Time) events.InsertEvent {
	return &JSONCompactEvent{
		row:        row,
		cols:       cols,
		table:      table,
		colNames:   names,
		readerTime: readerTime,
	}
}
