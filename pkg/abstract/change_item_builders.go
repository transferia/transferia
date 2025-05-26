package abstract

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"go.ytsaurus.tech/yt/go/schema"
)

func MakeInitTableLoad(pos LogPosition, table TableDescription, commitTime time.Time, tableSchema *TableSchema) []ChangeItem {
	return []ChangeItem{{
		ID:           pos.ID,
		TxID:         pos.TxID,
		LSN:          pos.LSN,
		CommitTime:   uint64(commitTime.UnixNano()),
		Table:        table.Name,
		Schema:       table.Schema,
		PartID:       table.PartID(),
		Kind:         InitTableLoad,
		TableSchema:  tableSchema,
		ColumnNames:  []string{},
		ColumnValues: []interface{}{},
	}}
}

func MakeDoneTableLoad(pos LogPosition, table TableDescription, commitTime time.Time, tableSchema *TableSchema) []ChangeItem {
	return []ChangeItem{{
		ID:           pos.ID,
		TxID:         pos.TxID,
		LSN:          pos.LSN,
		CommitTime:   uint64(commitTime.UnixNano()),
		Table:        table.Name,
		Schema:       table.Schema,
		PartID:       table.PartID(),
		Kind:         DoneTableLoad,
		TableSchema:  tableSchema,
		ColumnNames:  []string{},
		ColumnValues: []interface{}{},
	}}
}

func MakeTxDone(txSequence uint32, lsn uint64, execTS time.Time, lastPushedGTID, gtidStr string) ChangeItem {
	return ChangeItem{
		ID:           txSequence,
		LSN:          lsn,
		CommitTime:   uint64(execTS.UnixNano()),
		Counter:      0,
		Kind:         DDLKind,
		Schema:       "",
		Table:        "",
		PartID:       "",
		ColumnNames:  []string{"query"},
		ColumnValues: []interface{}{fmt.Sprintf("-- transaction %v done", lastPushedGTID)},
		TableSchema:  NewTableSchema([]ColSchema{{}}),
		OldKeys:      *new(OldKeysType),
		TxID:         gtidStr,
		Query:        "",
		Size:         RawEventSize(0),
	}
}

func MakeSynchronizeEvent() ChangeItem {
	return ChangeItem{
		ID:           0,
		Kind:         SynchronizeKind,
		Counter:      0,
		CommitTime:   uint64(time.Now().UnixNano()),
		LSN:          0,
		TableSchema:  nil,
		ColumnNames:  nil,
		Schema:       "",
		OldKeys:      EmptyOldKeys(),
		TxID:         "",
		Query:        "",
		Table:        "",
		ColumnValues: []interface{}{},
		Size:         EmptyEventSize(),
		PartID:       "",
	}
}

//---

// DefaultValue returns a default instance of the type represented by this schema. This method only works safely in heterogenous transfers.
func DefaultValue(c *changeitem.ColSchema) interface{} {
	switch schema.Type(c.DataType) {
	case schema.TypeInt64, schema.TypeInt32, schema.TypeInt16, schema.TypeInt8, schema.TypeUint64, schema.TypeUint32, schema.TypeUint16, schema.TypeUint8:
		return Restore(*c, float64(0))
	case schema.TypeFloat32:
		return float32(0)
	case schema.TypeFloat64:
		return float64(0)
	case schema.TypeBytes, schema.TypeString:
		return ""
	case schema.TypeBoolean:
		return false
	case schema.TypeAny:
		return Restore(*c, "{}")
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return time.Unix(0, 0)
	case schema.TypeInterval:
		return time.Duration(0)
	default:
		return nil
	}
}
