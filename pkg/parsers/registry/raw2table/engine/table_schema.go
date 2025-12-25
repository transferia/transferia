package engine

import (
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	ColNameTopic     = "topic"
	ColNamePartition = "partition"
	ColNameOffset    = "offset"
	ColNameTimestamp = "timestamp"
	Headers          = "headers"
	ColNameKey       = "key"
	ColNameValue     = "value"
)

func newColSchema(tableName, columnName string, dataType ytschema.Type, isPrimaryKey, required bool) abstract.ColSchema {
	return abstract.ColSchema{
		TableSchema:  "",
		TableName:    tableName,
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

func buildTableSchemaAndColumnNames(cfg *kafkaConfig) (*abstract.TableSchema, []string) {
	columns := []abstract.ColSchema{
		newColSchema(cfg.tableName, ColNameTopic, ytschema.TypeString, true, true),
		newColSchema(cfg.tableName, ColNamePartition, ytschema.TypeUint32, true, true),
		newColSchema(cfg.tableName, ColNameOffset, ytschema.TypeUint32, true, true),
	}

	if cfg.isAddTimestamp {
		columns = append(columns, newColSchema(cfg.tableName, ColNameTimestamp, ytschema.TypeTimestamp, false, true))
	}
	if cfg.isAddHeaders {
		columns = append(columns, newColSchema(cfg.tableName, Headers, ytschema.TypeAny, false, false))
	}

	if cfg.isAddKey {
		if cfg.isKeyString {
			columns = append(columns, newColSchema(cfg.tableName, ColNameKey, ytschema.TypeString, false, false))
		} else {
			columns = append(columns, newColSchema(cfg.tableName, ColNameKey, ytschema.TypeBytes, false, false))
		}
	}

	if cfg.isValueString {
		columns = append(columns, newColSchema(cfg.tableName, ColNameValue, ytschema.TypeString, false, false))
	} else {
		columns = append(columns, newColSchema(cfg.tableName, ColNameValue, ytschema.TypeBytes, false, false))
	}

	tableSchema := abstract.NewTableSchema(columns)
	columnNames := tableSchema.ColumnNames()
	return tableSchema, columnNames
}
