package schema

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

type TableDDL struct {
	tableID abstract.TableID
	sql     string
	engine  string
}

func (t *TableDDL) ToChangeItem() abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:               0,
		LSN:              0,
		CommitTime:       uint64(time.Now().UnixNano()),
		Counter:          0,
		Kind:             abstract.ChCreateTableKind,
		Schema:           t.tableID.Namespace,
		Table:            t.tableID.Name,
		PartID:           "",
		ColumnValues:     []interface{}{t.sql, t.engine},
		ColumnNames:      nil,
		TableSchema:      nil,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.EmptyEventSize(),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

func (t *TableDDL) SQL() string {
	return t.sql
}

func (t *TableDDL) Engine() string {
	return t.engine
}

func (t *TableDDL) TableID() abstract.TableID {
	return t.tableID
}

func (t *TableDDL) IsMatView() bool {
	return t.Engine() == "MaterializedView"
}

func NewTableDDL(tableID abstract.TableID, sql, engine string) *TableDDL {
	return &TableDDL{
		tableID: tableID,
		sql:     sql,
		engine:  engine,
	}
}
