package kafka

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	kafkaRawMessageTopic     = "topic"
	kafkaRawMessagePartition = "partition"
	kafkaRawMessageOffset    = "offset"
	kafkaRawMessageWriteTime = "write_time"
	kafkaRawMessageKey       = "key"
	kafkaRawMessageData      = "data"
)

var (
	kafkaRawDataSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: kafkaRawMessageTopic, DataType: ytschema.TypeString.String(), PrimaryKey: true, Required: true},
		{ColumnName: kafkaRawMessagePartition, DataType: ytschema.TypeUint32.String(), PrimaryKey: true, Required: true},
		{ColumnName: kafkaRawMessageOffset, DataType: ytschema.TypeUint64.String(), PrimaryKey: true, Required: true},
		{ColumnName: kafkaRawMessageWriteTime, DataType: ytschema.TypeDatetime.String(), PrimaryKey: true, Required: true},
		{ColumnName: kafkaRawMessageKey, DataType: ytschema.TypeBytes.String()},
		{ColumnName: kafkaRawMessageData, DataType: ytschema.TypeBytes.String()},
	})
	kafkaRawDataColumns = []string{kafkaRawMessageTopic, kafkaRawMessagePartition, kafkaRawMessageOffset, kafkaRawMessageWriteTime, kafkaRawMessageKey, kafkaRawMessageData}
	kafkaRawDataColsIDX = abstract.ColIDX(kafkaRawDataSchema.Columns())
)

func IsKafkaRawMessage(items []abstract.ChangeItem) bool {
	if len(items) == 0 {
		return false
	}
	return items[0].TableSchema == kafkaRawDataSchema
}

func MakeKafkaRawMessage(table string, commitTime time.Time, topic string, shard int, offset int64, key, data []byte) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         uint64(offset),
		CommitTime:  uint64(commitTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       table,
		PartID:      "",
		ColumnNames: kafkaRawDataColumns,
		ColumnValues: []interface{}{
			topic,
			shard,
			uint64(offset),
			commitTime,
			key,
			data,
		},
		TableSchema:      kafkaRawDataSchema,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.RawEventSize(uint64(len(data))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

func GetKafkaRawMessageKey(r *abstract.ChangeItem) []byte {
	return r.ColumnValues[kafkaRawDataColsIDX[kafkaRawMessageKey]].([]byte)
}

func GetKafkaRawMessageData(r *abstract.ChangeItem) []byte {
	return r.ColumnValues[kafkaRawDataColsIDX[kafkaRawMessageData]].([]byte)
}
