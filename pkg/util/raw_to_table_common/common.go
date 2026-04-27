package raw_to_table_common

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type CommonConfig struct {
	TableName string // by default (if empty string), tableName = topicName

	IsKeyEnabled bool
	KeyType      DataType

	ValueType DataType

	IsTimestampEnabled bool
	IsHeadersEnabled   bool

	// Suffix for the DLQ table name: final name is baseTable + normalized suffix
	// Empty uses "_dlq"
	DLQSuffix string
}

func (c *CommonConfig) Validate() error {
	if c.IsKeyEnabled {
		switch c.KeyType {
		case String, Bytes, JSON:
		default:
			return xerrors.Errorf("invalid parser config - unsupported key type: %s", c.KeyType.String())
		}
	}

	switch c.ValueType {
	case String, Bytes, JSON:
	default:
		return xerrors.Errorf("invalid parser config - unsupported value type: %s", c.ValueType.String())
	}

	return nil
}
