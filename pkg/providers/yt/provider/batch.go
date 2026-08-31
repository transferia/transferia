package provider

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
)

// decodedRow is a raw row produced by the parallel table reader.
type decodedRow struct {
	values  []interface{}
	rowIDX  int64
	rawSize int
}

func (d *decodedRow) RawSize() int {
	return d.rawSize
}

// batch accumulates decoded rows and materializes them as abstract.ChangeItem
// slices for delivery through abstract.Pusher. The table schema and column
// names are resolved once in newEmptyBatch and reused for every appended row.
//
// schema and tableName are propagated from the caller's TableDescription
// rather than derived from tbl, because the async CH sink correlates data
// rows with the InitTableLoad control event by full TablePartID equality
// (TableID{Namespace, Name} + PartID). yt_table.YtTable.Schema() is a stub
// that returns "", which would break that correlation.
type batch struct {
	tbl         yt_table.YtTable
	schema      string
	tableName   string
	partID      string
	idxCol      string
	tableSchema *abstract.TableSchema
	colNames    []string
	changes     []abstract.ChangeItem
	byteSize    int
}

func newEmptyBatch(tbl yt_table.YtTable, cap int, schema, tableName, partID, idxCol string) (*batch, error) {
	oldTable, err := tbl.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("table cannot be converted to old format: %w", err)
	}
	colNames, err := tbl.ColumnNames()
	if err != nil {
		return nil, xerrors.Errorf("error getting column names: %w", err)
	}
	return &batch{
		tbl:         tbl,
		schema:      schema,
		tableName:   tableName,
		partID:      partID,
		idxCol:      idxCol,
		tableSchema: oldTable,
		colNames:    colNames,
		changes:     make([]abstract.ChangeItem, 0, cap),
		byteSize:    0,
	}, nil
}

// Append constructs an InsertKind ChangeItem from the decoded row using the
// batch's memoized schema/column names. The produced item mirrors what the
// legacy events.ToOldChangeItem builder emitted.
func (b *batch) Append(row decodedRow) {
	b.changes = append(b.changes, abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   0,
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       b.schema,
		Table:        b.tableName,
		PartID:       b.partID,
		ColumnNames:  b.colNames,
		ColumnValues: row.values,
		TableSchema:  b.tableSchema,
		OldKeys: abstract.OldKeysType{
			KeyNames:  nil,
			KeyTypes:  nil,
			KeyValues: nil,
		},
		Size:             abstract.RawEventSize(uint64(row.rawSize)),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	})
	b.byteSize += row.rawSize
}

func (b *batch) Items() []abstract.ChangeItem {
	return b.changes
}

func (b *batch) Len() int {
	return len(b.changes)
}

func (b *batch) Size() int {
	return b.byteSize
}
