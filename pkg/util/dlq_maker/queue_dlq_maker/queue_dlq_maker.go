package queue_dlq_maker

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/util/raw_to_table_common"
)

const defaultDLQSuffix = "_dlq"

// QueueDLQMaker produces rows for the dead-letter table. That table intentionally uses a fixed,
// wider layout than the main sink: it always has timestamp, headers, key, and value columns
// (key and value as raw bytes), plus failure_reason. This preserves the full original
// message for inspection and does not mirror the user’s main-table toggles or typed key/value
// settings.
type QueueDLQMaker struct {
	tableName   string
	dlqSuffix   string
	tableSchema *abstract.TableSchema
	columnNames []string
}

func (m *QueueDLQMaker) BuildDLQChangeItem(msg parsers.Message, partition abstract.Partition, failureReasonValue string) abstract.ChangeItem {
	baseTable := m.tableName
	if baseTable == "" {
		baseTable = partition.Topic
	}
	tableName := baseTable + m.dlqSuffix
	columnValues := raw_to_table_common.BuildColumnValues(msg, partition, true, raw_to_table_common.Bytes, raw_to_table_common.Bytes, true, true, m.columnNames)
	columnValues = append(columnValues, failureReasonValue)
	result := raw_to_table_common.BuildChangeItem(
		tableName,
		msg,
		partition,
		m.columnNames,
		columnValues,
		m.tableSchema,
	)
	return result
}

func NewQueueDLQMaker(tableName string, dlqSuffix string) *QueueDLQMaker {
	currDlqSuffix := dlqSuffix
	if currDlqSuffix == "" {
		currDlqSuffix = defaultDLQSuffix
	}
	dlqConfig := &raw_to_table_common.CommonConfig{
		TableName:          tableName,
		IsKeyEnabled:       true,
		KeyType:            raw_to_table_common.Bytes,
		ValueType:          raw_to_table_common.Bytes,
		IsTimestampEnabled: true,
		IsHeadersEnabled:   true,
		DLQSuffix:          currDlqSuffix,
	}
	tableSchema, columnNames := raw_to_table_common.BuildTableSchemaAndColumnNames(dlqConfig, true)

	return &QueueDLQMaker{
		tableName:   tableName,
		dlqSuffix:   currDlqSuffix,
		tableSchema: tableSchema,
		columnNames: columnNames,
	}
}
