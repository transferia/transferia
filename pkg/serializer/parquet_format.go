package serializer

import (
	"encoding/json"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

var primitiveTypesMap = map[schema.Type]parquet.Node{
	schema.TypeInt8:  parquet.Int(8),
	schema.TypeInt16: parquet.Int(16),
	schema.TypeInt32: parquet.Int(32),
	schema.TypeInt64: parquet.Int(64),

	schema.TypeUint8:  parquet.Uint(8),
	schema.TypeUint16: parquet.Uint(16),
	schema.TypeUint32: parquet.Uint(32),
	schema.TypeUint64: parquet.Uint(64),

	schema.TypeBoolean: parquet.Leaf(parquet.BooleanType),

	schema.TypeFloat32: parquet.Leaf(parquet.FloatType),
	schema.TypeFloat64: parquet.String(), // stringed decimal. todo fixme

	schema.TypeTimestamp: parquet.Timestamp(parquet.Nanosecond),

	schema.TypeDate:     parquet.Date(),
	schema.TypeDatetime: parquet.Timestamp(parquet.Nanosecond),
	schema.TypeInterval: parquet.Timestamp(parquet.Nanosecond),

	schema.TypeBytes:  parquet.String(),
	schema.TypeString: parquet.String(),
	schema.TypeAny:    parquet.JSON(),
}

// Parses map[string] -> parquet.Row. Doesn't support repeated
// fields or composite values. Json fields are saved as string
func toParquetValue(column parquet.Field, col abstract.ColSchema, value any, idx int) (*parquet.Value, error) {
	defLevel := 0
	var leafValue parquet.Value
	if value == nil {
		leafValue = parquet.ValueOf(nil)
		switch schema.Type(col.DataType) {
		case schema.TypeBytes, schema.TypeString:
			leafValue = parquet.ValueOf("")
		}
	} else {
		if !column.Required() {
			defLevel++
		}
		switch schema.Type(col.DataType) {
		case schema.TypeFloat64:
			// we store all doubles as string, some storage may pass for us float64 instead of json.Number
			// so we must stringify it
			switch value.(type) {
			case string:
				leafValue = parquet.ValueOf(value)
			default:
				leafValue = parquet.ValueOf(fmt.Sprintf("%v", value))
			}
		case schema.TypeAny:
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

func buildParquetGroup(tableSchema abstract.FastTableSchema) (parquet.Group, error) {
	groupNode := parquet.Group{}

	for name, colSchema := range tableSchema {
		var n parquet.Node

		if value, contains := primitiveTypesMap[schema.Type(colSchema.DataType)]; contains {
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
