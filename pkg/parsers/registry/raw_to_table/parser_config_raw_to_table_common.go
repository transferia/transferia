package raw_to_table

import (
	"github.com/transferia/transferia/pkg/util/raw_to_table_common"
)

// ParserConfigRawToTableCommon is the full raw-to-table parser config (optional key column).
//
// DLQ table layout is fixed and wider than the main table; see package doc.
type ParserConfigRawToTableCommon struct {
	TableName string // by default (if empty string), tableName = topicName

	IsKeyEnabled bool
	KeyType      raw_to_table_common.DataType

	ValueType raw_to_table_common.DataType

	IsTimestampEnabled bool
	IsHeadersEnabled   bool

	// Suffix for the DLQ table name (base + suffix). Empty → "_dlq"; a value without a leading '_' gets one prepended.
	// Rows in that table always include timestamp, headers, key/value as bytes, and failure_reason, independent of the flags above.
	DLQSuffix string
}

func (c *ParserConfigRawToTableCommon) IsNewParserConfig() {}

func (c *ParserConfigRawToTableCommon) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigRawToTableCommon) toCommonConfig() *raw_to_table_common.CommonConfig {
	return &raw_to_table_common.CommonConfig{
		TableName: c.TableName,

		IsKeyEnabled: c.IsKeyEnabled,
		KeyType:      c.KeyType,

		ValueType: c.ValueType,

		IsTimestampEnabled: c.IsTimestampEnabled,
		IsHeadersEnabled:   c.IsHeadersEnabled,

		DLQSuffix: c.DLQSuffix,
	}
}

func (c *ParserConfigRawToTableCommon) Validate() error {
	config := c.toCommonConfig()
	return config.Validate()
}
