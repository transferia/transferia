package changeitem

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	ytschema "go.ytsaurus.tech/yt/go/schema"
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
		{ColumnName: RawMessageTopic, DataType: string(ytschema.TypeString), PrimaryKey: true, Required: true},
		{ColumnName: RawMessagePartition, DataType: string(ytschema.TypeUint32), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageSeqNo, DataType: string(ytschema.TypeUint64), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageWriteTime, DataType: string(ytschema.TypeDatetime), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageData, DataType: string(ytschema.TypeString), OriginalType: OriginalTypeMirrorBinary},
		{ColumnName: RawMessageMeta, DataType: string(ytschema.TypeAny)},
		{ColumnName: RawSequenceKey, DataType: string(ytschema.TypeBytes)},
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

func GetRawMessagePartition(changeItem ChangeItem) int {
	return changeItem.ColumnValues[RawDataColsIDX[RawMessagePartition]].(int)
}

func GetRawMessageSeqNo(changeItem ChangeItem) uint64 {
	return changeItem.ColumnValues[RawDataColsIDX[RawMessageSeqNo]].(uint64)
}

func GetRawMessageWriteTime(changeItem ChangeItem) time.Time {
	return changeItem.ColumnValues[RawDataColsIDX[RawMessageWriteTime]].(time.Time)
}

func GetRawMessageSequenceKey(changeItem ChangeItem) ([]byte, error) {
	if changeItem.TableSchema != RawDataSchema {
		return nil, xerrors.Errorf("changeItem should be 'mirror'")
	}
	return changeItem.ColumnValues[RawDataColsIDX[RawSequenceKey]].([]byte), nil
}

func GetRawMessageData(changeItem ChangeItem) ([]byte, error) {
	switch v := changeItem.ColumnValues[RawDataColsIDX[RawMessageData]].(type) {
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
