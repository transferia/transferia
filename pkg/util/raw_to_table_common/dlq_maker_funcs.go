package raw_to_table_common

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/util/jsonx"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func BuildChangeItem(
	tableName string,
	msg parsers.Message,
	partition abstract.Partition,
	columnNames []string,
	columnValues []any,
	tableSchema *abstract.TableSchema,
) abstract.ChangeItem {
	table := tableName
	if table == "" {
		table = partition.Topic
	}
	return abstract.ChangeItem{
		ID:               0,
		LSN:              msg.Offset,
		CommitTime:       uint64(msg.WriteTime.UnixNano()),
		Counter:          0,
		Kind:             abstract.InsertKind,
		Schema:           "",
		Table:            table,
		PartID:           "",
		ColumnNames:      columnNames,
		ColumnValues:     columnValues,
		TableSchema:      tableSchema,
		OldKeys:          abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		Size:             abstract.RawEventSize(uint64(len(msg.Value))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: partition.Topic, PartitionNum: int(partition.Partition), Offset: msg.Offset, Index: 0},
	}
}

func BuildColumnValues(
	msg parsers.Message,
	partition abstract.Partition,
	IsKeyEnabled bool,
	keyType DataType,
	valueType DataType,
	IsTimestampEnabled bool,
	IsHeadersEnabled bool,
	columnNames []string,
) []any {
	columnValues := make([]any, 0, len(columnNames))
	columnValues = append(columnValues, partition.Topic)
	columnValues = append(columnValues, partition.Partition)
	columnValues = append(columnValues, uint32(msg.Offset))
	if IsTimestampEnabled {
		columnValues = append(columnValues, msg.WriteTime)
	}
	if IsHeadersEnabled {
		columnValues = append(columnValues, msg.Headers)
	}

	appendProperType := func(configuredType DataType, val []byte, outColumnValues []any) []any {
		if val == nil {
			return append(outColumnValues, nil)
		}
		switch configuredType {
		case String:
			return append(outColumnValues, string(val))
		case Bytes:
			return append(outColumnValues, val)
		case JSON:
			var value any
			_ = jsonx.Unmarshal(val, &value) // this case caught at earlier stage - 'sendToDLQReason'
			return append(outColumnValues, value)
		default:
			return outColumnValues
		}
	}

	if IsKeyEnabled {
		columnValues = appendProperType(keyType, msg.Key, columnValues)
	}
	columnValues = appendProperType(valueType, msg.Value, columnValues)
	return columnValues
}

func BuildTableSchemaAndColumnNames(cfg *CommonConfig, isDLQ bool) (*abstract.TableSchema, []string) {
	columns := []abstract.ColSchema{
		newColSchema(ColNameTopic, ytschema.TypeString, true, true),
		newColSchema(ColNamePartition, ytschema.TypeUint32, true, true),
		newColSchema(ColNameOffset, ytschema.TypeUint32, true, true),
	}

	if cfg.IsTimestampEnabled {
		columns = append(columns, newColSchema(ColNameTimestamp, ytschema.TypeTimestamp, false, true))
	}
	if cfg.IsHeadersEnabled {
		columns = append(columns, newColSchema(ColNameHeaders, ytschema.TypeAny, false, false))
	}

	configuredTypeToYtType := map[DataType]ytschema.Type{
		Bytes:  ytschema.TypeBytes,
		String: ytschema.TypeString,
		JSON:   ytschema.TypeAny,
	}

	if cfg.IsKeyEnabled {
		keyYtType := configuredTypeToYtType[cfg.KeyType]
		columns = append(columns, newColSchema(ColNameKey, keyYtType, false, false))
	}

	valueYtType := configuredTypeToYtType[cfg.ValueType]
	columns = append(columns, newColSchema(ColNameValue, valueYtType, false, false))

	if isDLQ {
		columns = append(columns, newColSchema(ColNameFailureReason, ytschema.TypeString, false, false))
	}

	tableSchema := abstract.NewTableSchema(columns)
	columnNames := tableSchema.ColumnNames()
	return tableSchema, columnNames
}
