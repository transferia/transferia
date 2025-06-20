//go:build !disable_clickhouse_provider

package format

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/adapter"
	"github.com/transferia/transferia/pkg/base/events"
)

type CSVEvent struct {
	row        []byte
	cols       *abstract.TableSchema
	table      abstract.TableID
	colNames   []string
	readerTime time.Time
}

func (e *CSVEvent) ToOldChangeItem() (*abstract.ChangeItem, error) {
	// this method for debug only purpose, we don't have good csv parser yet
	csvRow := strings.Split(string(e.row), ",")
	row := make([]interface{}, len(e.colNames))
	for i, col := range e.cols.Columns() {
		var cell interface{}
		if len(csvRow) <= i {
			continue
		}
		if csvRow[i] == "\\N" {
			cell = nil
			continue
		} else {
			if err := json.Unmarshal([]byte(csvRow[i]), &cell); err != nil {
				continue
			}
		}
		switch strings.ToLower(col.DataType) {
		case "date", "datetime", "timestamp":
			strCell, ok := cell.(string)
			if !ok {
				strCell = csvRow[i]
			}
			res, err := dateparse.ParseAny(strCell)
			if err != nil {
				continue
			}
			row[i] = abstract.Restore(col, res)
		default:
			row[i] = abstract.Restore(col, cell)
		}
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

func (e *CSVEvent) Table() base.Table {
	return adapter.NewTableFromLegacy(e.cols, e.table)
}

func (e *CSVEvent) NewValuesCount() int {
	return len(e.cols.Columns())
}

func (e *CSVEvent) NewValue(i int) (base.Value, error) {
	return nil, xerrors.New("compact json event NewValue not implemented")
}

func NewCSVEvent(row []byte, cols *abstract.TableSchema, names []string, table abstract.TableID, readerTime time.Time) events.InsertEvent {
	return &CSVEvent{
		row:        row,
		cols:       cols,
		table:      table,
		colNames:   names,
		readerTime: readerTime,
	}
}
