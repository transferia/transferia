//go:build !disable_clickhouse_provider

package schema

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
)

const matViewEngine = "MaterializedView"

const (
	// ClickhouseDDLTimeout is default value of distributed_ddl_task_timeout CH setting, used as fallback
	ClickhouseDDLTimeout = 180
	// DDLTimeoutCorrection is added to DDL query timeout to have a chance to catch CH error, instead of ContextDeadlineExceeded
	DDLTimeoutCorrection  = 2
	ClickhouseReadTimeout = 30 * time.Second
)

type TableDDL struct {
	tableID abstract.TableID
	sql     string
	engine  string
}

func (t TableDDL) ToChangeItem() abstract.ChangeItem {
	sql := t.sql
	kind := abstract.ChCreateTableKind
	if IsDistributedDDL(sql) {
		sql = ReplaceCluster(sql, "{cluster}")
		kind = abstract.ChCreateTableDistributedKind
	}
	return abstract.ChangeItem{
		Schema:       t.tableID.Namespace,
		Table:        t.tableID.Name,
		PartID:       "",
		Kind:         kind,
		CommitTime:   uint64(time.Now().UnixNano()),
		ColumnValues: []interface{}{sql, t.engine},
		ID:           0,
		LSN:          0,
		Counter:      0,
		ColumnNames:  nil,
		TableSchema:  nil,
		OldKeys:      abstract.EmptyOldKeys(),
		TxID:         "",
		Query:        "",
		Size:         abstract.EmptyEventSize(),
	}
}

func (t TableDDL) SQL() string {
	return t.sql
}

func (t TableDDL) Engine() string {
	return t.engine
}

func (t TableDDL) TableID() abstract.TableID {
	return t.tableID
}

func (t TableDDL) IsMatView() bool {
	return t.Engine() == matViewEngine
}

func NewTableDDL(tableID abstract.TableID, sql, engine string) TableDDL {
	return TableDDL{
		tableID: tableID,
		sql:     sql,
		engine:  engine,
	}
}
