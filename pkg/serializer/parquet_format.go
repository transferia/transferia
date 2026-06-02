package serializer

import (
	"encoding/json"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var primitiveTypesMap = map[ytschema.Type]parquet.Node{
	ytschema.TypeInt8:  parquet.Int(8),
	ytschema.TypeInt16: parquet.Int(16),
	ytschema.TypeInt32: parquet.Int(32),
	ytschema.TypeInt64: parquet.Int(64),

	ytschema.TypeUint8:  parquet.Uint(8),
	ytschema.TypeUint16: parquet.Uint(16),
	ytschema.TypeUint32: parquet.Uint(32),
	ytschema.TypeUint64: parquet.Uint(64),

	ytschema.TypeBoolean: parquet.Leaf(parquet.BooleanType),

	ytschema.TypeFloat32: parquet.Leaf(parquet.FloatType),
	ytschema.TypeFloat64: parquet.String(), // stringed decimal. todo fixme

	ytschema.TypeDate:      parquet.Date(),
	ytschema.TypeDatetime:  parquet.Timestamp(parquet.Nanosecond),
	ytschema.TypeTimestamp: parquet.Timestamp(parquet.Nanosecond),

	ytschema.TypeInterval: parquet.Timestamp(parquet.Nanosecond),

	ytschema.TypeBytes:  parquet.Leaf(parquet.ByteArrayType),
	ytschema.TypeString: parquet.String(),
	ytschema.TypeAny:    parquet.JSON(),
}

// Parses map[string] -> parquet.Row. Doesn't support repeated
// fields or composite values. Json fields are saved as string
func toParquetValue(column parquet.Field, col abstract.ColSchema, value any, idx int) (*parquet.Value, error) {
	defLevel := 0
	var leafValue parquet.Value
	if value == nil {
		leafValue = parquet.ValueOf(nil)
		switch ytschema.Type(col.DataType) {
		case ytschema.TypeBytes, ytschema.TypeString:
			leafValue = parquet.ValueOf("")
		}
	} else {
		if !column.Required() {
			defLevel++
		}
		switch ytschema.Type(col.DataType) {
		case ytschema.TypeFloat64:
			// we store all doubles as string, some storage may pass for us float64 instead of json.Number
			// so we must stringify it
			switch value.(type) {
			case string:
				leafValue = parquet.ValueOf(value)
			default:
				leafValue = parquet.ValueOf(fmt.Sprintf("%v", value))
			}
		case ytschema.TypeAny:
			marshalled, err := json.Marshal(value)
			if err != nil {
				return nil, xerrors.Errorf("serializer:parquet: field %v type of '%v' failed to marshal: %w", column.Name(), column.Type().String(), err)
			}
			leafValue = parquet.ValueOf(marshalled)
		default:
			leafValue = parquet.ValueOf(value)
		}
	}

	leafValue = leafValue.Level(0, defLevel, idx)
	return &leafValue, nil
}

func toParquetRows(items []*abstract.ChangeItem, schema *parquet.Schema, tableSchema abstract.FastTableSchema) ([]parquet.Row, error) {
	rows := make([]parquet.Row, len(items))
	for i, item := range items {
		var row []parquet.Value
		rowMap := item.AsMap()
		for idx, field := range schema.Fields() {
			v, err := toParquetValue(field, tableSchema[abstract.ColumnName(field.Name())], rowMap[field.Name()], idx)
			if err != nil {
				return nil, xerrors.Errorf("unable to convert field %v to parquet value: %w", field.Name(), err)
			}
			row = append(row, *v)
		}
		rows[i] = row
	}
	return rows, nil
}

func buildParquetGroup(tableSchema abstract.FastTableSchema) (parquet.Group, error) {
	groupNode := parquet.Group{}

	for name, colSchema := range tableSchema {
		var n parquet.Node

		if value, contains := primitiveTypesMap[ytschema.Type(colSchema.DataType)]; contains {
			n = value
		} else {
			return nil, fmt.Errorf("serializer:parquet: field %v type of '%v' not recognised", name, colSchema.DataType)
		}
		// 	n = encodingFn(n) // TODO add specific encoding support

		if !colSchema.Required {
			n = parquet.Optional(n)
		}

		groupNode[string(name)] = n
	}

	return groupNode, nil

}

func BuildParquetSchema(tableSchema abstract.FastTableSchema) (*parquet.Schema, error) {
	node, err := buildParquetGroup(tableSchema)
	if err != nil {
		return nil, err
	}

	return parquet.NewSchema("table", node), nil
}
