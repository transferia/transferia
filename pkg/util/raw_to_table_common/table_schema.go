package raw_to_table_common

import (
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	ColNameTopic     = "topic"
	ColNamePartition = "partition"
	ColNameOffset    = "offset"
	ColNameTimestamp = "timestamp"
	ColNameHeaders   = "headers"
	ColNameKey       = "key"
	ColNameValue     = "value"

	ColNameFailureReason = "failure_reason"
)

func newColSchema(columnName string, dataType ytschema.Type, isPrimaryKey, required bool) abstract.ColSchema {
	return abstract.ColSchema{
		TableSchema:  "",
		TableName:    "",
		Path:         "",
		ColumnName:   columnName,
		DataType:     dataType.String(),
		PrimaryKey:   isPrimaryKey,
		FakeKey:      false,
		Required:     required,
		Expression:   "",
		OriginalType: "",
		Properties:   nil,
	}
}
