package changeitem

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	RawMessageTopic     = "topic"
	RawMessagePartition = "partition"
	RawMessageSeqNo     = "seq_no"
	RawMessageWriteTime = "write_time"
	RawMessageData      = "data"
	RawMessageMeta      = "meta"
	RawSequenceKey      = "sequence_key"

	OriginalTypeMirrorBinary = "mirror:binary"
)

var (
	RawDataSchema = NewTableSchema([]ColSchema{
		{ColumnName: RawMessageTopic, DataType: string(schema.TypeString), PrimaryKey: true, Required: true},
		{ColumnName: RawMessagePartition, DataType: string(schema.TypeUint32), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageSeqNo, DataType: string(schema.TypeUint64), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageWriteTime, DataType: string(schema.TypeDatetime), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageData, DataType: string(schema.TypeString), OriginalType: OriginalTypeMirrorBinary},
		{ColumnName: RawMessageMeta, DataType: string(schema.TypeAny)},
		{ColumnName: RawSequenceKey, DataType: string(schema.TypeBytes)},
	})
	RawDataColumns = RawDataSchema.Columns().ColumnNames()
	RawDataColsIDX = colIDX(RawDataSchema.Columns())
)

func MakeRawMessage(sequenceKey []byte, table string, commitTime time.Time, topic string, shard int, offset int64, data []byte) ChangeItem {
	return MakeRawMessageWithMeta(sequenceKey, table, commitTime, topic, shard, offset, data, nil)
}

func MakeRawMessageWithMeta(sequenceKey []byte, table string, commitTime time.Time, topic string, shard int, offset int64, data []byte, meta map[string]string) ChangeItem {
	return ChangeItem{
		ID:          0,
		LSN:         uint64(offset),
		CommitTime:  uint64(commitTime.UnixNano()),
		Counter:     0,
		Kind:        InsertKind,
		Schema:      "",
		Table:       table,
		PartID:      "",
		ColumnNames: RawDataColumns,
		ColumnValues: []interface{}{
			topic,
			shard,
			uint64(offset),
			commitTime,
			string(data),
			meta,
			sequenceKey,
		},
		TableSchema:      RawDataSchema,
		OldKeys:          EmptyOldKeys(),
		Size:             RawEventSize(uint64(len(data))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: QueueMessageMeta{TopicName: topic, PartitionNum: shard, Offset: uint64(offset), Index: 0},
	}
}

// getters

func GetSequenceKey(changeItem *ChangeItem) ([]byte, error) {
	if changeItem.TableSchema != RawDataSchema {
		return nil, xerrors.Errorf("changeItem should be 'mirror'")
	}
	return changeItem.ColumnValues[RawDataColsIDX[RawSequenceKey]].([]byte), nil
}

func GetRawMessageData(r ChangeItem) ([]byte, error) {
	switch v := r.ColumnValues[RawDataColsIDX[RawMessageData]].(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, xerrors.Errorf("unexpected data type: %T, expected string or []byte", v)
	}
}

// util

func colIDX(schema []ColSchema) map[string]int {
	res := map[string]int{}
	for i := range schema {
		res[schema[i].ColumnName] = i
	}
	return res
}
