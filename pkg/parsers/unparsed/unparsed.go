package unparsed

import (
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	ColNameTimestamp = "_timestamp"
	ColNamePartition = "_partition"
	ColNameOffset    = "_offset"
	ColNameIdx       = "_idx"
)

var (
	UnparsedSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{
			ColumnName: ColNameTimestamp,
			DataType:   ytschema.TypeTimestamp.String(),
			PrimaryKey: true,
			Required:   true,
		},
		{
			ColumnName: ColNamePartition,
			DataType:   ytschema.TypeBytes.String(),
			PrimaryKey: true,
			Required:   true,
		},
		{
			ColumnName: ColNameOffset,
			DataType:   ytschema.TypeUint64.String(),
			PrimaryKey: true,
			Required:   true,
		},
		{
			ColumnName: ColNameIdx,
			DataType:   ytschema.TypeUint32.String(),
			PrimaryKey: true,
			Required:   true,
		},
		{
			ColumnName: "unparsed_row",
			DataType:   ytschema.TypeBytes.String(),
		},
		{
			ColumnName: "reason",
			DataType:   ytschema.TypeBytes.String(),
		},
	})
	UnparsedCols = cols(UnparsedSchema.Columns())
)

func cols(schema []abstract.ColSchema) []string {
	result := make([]string, len(schema))
	for i := range schema {
		result[i] = schema[i].ColumnName
	}
	return result
}

func IsGenericUnparsedSchema(schema *abstract.TableSchema) bool {
	originalColumns := schema.Columns()
	unparsedColumns := UnparsedSchema.Columns()
	if len(originalColumns) != len(unparsedColumns) {
		return false
	}
	for i := range originalColumns {
		if originalColumns[i].ColumnName != unparsedColumns[i].ColumnName ||
			// type check contradicts timestamp hacks: https://github.com/transferia/transferia/arcadia/transfer_manager/go/pkg/providers/yt/sink/sink.go?rev=r13620609#L1018
			// originalColumns[i].DataType != unparsedColumns[i].DataType ||
			originalColumns[i].PrimaryKey != unparsedColumns[i].PrimaryKey ||
			originalColumns[i].Required != unparsedColumns[i].Required {
			return false
		}
	}
	return true
}

func TableName(partition abstract.Partition, name string) string {
	if name == "" {
		name = partition.Topic
	}
	return replaceProblemSymbols(name)
}

func replaceProblemSymbols(in string) string {
	result := in
	result = strings.ReplaceAll(result, "/", "_")
	result = strings.ReplaceAll(result, "@", "_")
	return result
}

func NewUnparsed(partition abstract.Partition, name string, line []byte, reason string, idx int, offset uint64, writeTime time.Time) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         offset,
		CommitTime:  uint64(writeTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       fmt.Sprintf("%v_unparsed", TableName(partition, name)),
		PartID:      "",
		ColumnNames: UnparsedCols,
		ColumnValues: []interface{}{
			time.Now(),
			partition.String(),
			offset,
			uint32(idx),
			line,
			reason,
		},
		TableSchema:      UnparsedSchema,
		OldKeys:          abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		Size:             abstract.RawEventSize(uint64(len(line))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}
