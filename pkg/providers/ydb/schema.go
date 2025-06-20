//go:build !disable_ydb_provider

package ydb

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"go.ytsaurus.tech/yt/go/schema"
)

type column struct {
	Name string
	Type string
}

func buildColumnDescription(col *column, isPkey bool) abstract.ColSchema {
	ydbTypeStr := col.Type
	isOptional := strings.Contains(ydbTypeStr, "Optional") || strings.Contains(ydbTypeStr, "?")
	ydbTypeStr = strings.ReplaceAll(ydbTypeStr, "?", "")
	ydbTypeStr = strings.ReplaceAll(ydbTypeStr, "Optional<", "")
	ydbTypeStr = strings.ReplaceAll(ydbTypeStr, ">", "")
	if bracketsStart := strings.Index(ydbTypeStr, "("); bracketsStart > 0 {
		ydbTypeStr = ydbTypeStr[:bracketsStart]
	}

	var dataType schema.Type
	switch ydbTypeStr {
	case "Bool":
		dataType = schema.TypeBoolean
	case "Int8":
		dataType = schema.TypeInt8
	case "Int16":
		dataType = schema.TypeInt16
	case "Int32":
		dataType = schema.TypeInt32
	case "Int64":
		dataType = schema.TypeInt64
	case "Uint8":
		dataType = schema.TypeUint8
	case "Uint16":
		dataType = schema.TypeUint16
	case "Uint32":
		dataType = schema.TypeUint32
	case "Uint64":
		dataType = schema.TypeUint64
	case "Float":
		dataType = schema.TypeFloat32
	case "Double":
		dataType = schema.TypeFloat64
	case "String":
		dataType = schema.TypeBytes
	case "Utf8", "Decimal", "DyNumber":
		dataType = schema.TypeString
	case "Date":
		dataType = schema.TypeDate
	case "Datetime":
		dataType = schema.TypeDatetime
	case "Timestamp":
		dataType = schema.TypeTimestamp
	case "Interval":
		dataType = schema.TypeInterval
	case "Uuid":
		dataType = schema.TypeString
	default:
		dataType = schema.TypeAny
	}

	return abstract.ColSchema{
		ColumnName:   col.Name,
		DataType:     string(dataType),
		Required:     !isOptional,
		OriginalType: "ydb:" + ydbTypeStr,
		PrimaryKey:   isPkey,
		TableSchema:  "",
		TableName:    "",
		Path:         "",
		FakeKey:      false,
		Expression:   "",
		Properties:   nil,
	}
}

func fromYdbSchemaImpl(original []column, keys []string) abstract.TableColumns {
	columnNameToPKey := map[string]bool{}
	for _, k := range keys {
		columnNameToPKey[k] = true
	}
	columnNameToIndex := make(map[string]int)
	for i, el := range original {
		columnNameToIndex[el.Name] = i
	}

	result := make([]abstract.ColSchema, 0, len(original))
	for _, currentKey := range keys {
		index := columnNameToIndex[currentKey]
		result = append(result, buildColumnDescription(&original[index], true))
	}
	for i, currentColumn := range original {
		if !columnNameToPKey[currentColumn.Name] {
			result = append(result, buildColumnDescription(&original[i], false))
		}
	}
	return result
}

func FromYdbSchema(original []options.Column, keys []string) abstract.TableColumns {
	columns := make([]column, len(original))
	for i, el := range original {
		columns[i] = column{
			Name: el.Name,
			Type: fmt.Sprintf("%v", el.Type),
		}
	}
	return fromYdbSchemaImpl(columns, keys)
}
