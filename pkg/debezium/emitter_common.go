package debezium

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	"github.com/transferria/transferria/pkg/util"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func extractSignedInt(val interface{}) (int64, error) {
	switch t := val.(type) {
	case int:
		return int64(t), nil
	case int8:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case json.Number:
		return t.Int64()
	default:
		return 0, xerrors.Errorf("unknown input data type for signed int")
	}
}

func extractUnsignedInt(val interface{}) (uint64, error) {
	switch t := val.(type) {
	case int:
		return uint64(t), nil
	case uint:
		return uint64(t), nil
	case int8:
		return uint64(t), nil
	case uint8:
		return uint64(t), nil
	case int16:
		return uint64(t), nil
	case uint16:
		return uint64(t), nil
	case int32:
		return uint64(t), nil
	case uint32:
		return uint64(t), nil
	case int64:
		return uint64(t), nil
	case uint64:
		return t, nil
	case json.Number:
		s, err := strconv.ParseUint(t.String(), 10, 64)
		if err != nil {
			return 0, xerrors.Errorf("unable to parse uint64, err: %w", err)
		}
		return s, nil
	default:
		return 0, xerrors.Errorf("unknown input data type for unsigned int")
	}
}

func addCommon(v *debeziumcommon.Values, colSchema *abstract.ColSchema, colVal interface{}) error {
	if colVal == nil {
		v.AddVal(colSchema.ColumnName, nil)
		return nil
	}

	switch colSchema.DataType {
	case string(ytschema.TypeInt64), string(ytschema.TypeInt32), string(ytschema.TypeInt16), string(ytschema.TypeInt8):
		result, err := extractSignedInt(colVal)
		if err != nil {
			return xerrors.Errorf("unable for extract signed int for type: %T", result)
		}
		v.AddVal(colSchema.ColumnName, colVal)

	case string(ytschema.TypeUint64), string(ytschema.TypeUint32), string(ytschema.TypeUint16), string(ytschema.TypeUint8):
		result, err := extractUnsignedInt(colVal)
		if err != nil {
			return xerrors.Errorf("unable for extract unsigned int for type: %T", colVal)
		}
		v.AddVal(colSchema.ColumnName, result)

	case string(ytschema.TypeFloat32), string(ytschema.TypeFloat64):
		switch t := colVal.(type) {
		case float32:
			v.AddVal(colSchema.ColumnName, t)
		case float64:
			v.AddVal(colSchema.ColumnName, t)
		case json.Number:
			v.AddVal(colSchema.ColumnName, t)
		default:
			return xerrors.Errorf("unknown input data type for type float: %T", colVal)
		}

	case string(ytschema.TypeBytes):
		switch t := colVal.(type) {
		case string:
			v.AddVal(colSchema.ColumnName, base64.StdEncoding.EncodeToString([]byte(t)))
		case []byte:
			v.AddVal(colSchema.ColumnName, base64.StdEncoding.EncodeToString(t))
		default:
			return xerrors.Errorf("unknown input data type for type bytes(yt:string): %T", colVal)
		}

	case string(ytschema.TypeString):
		switch t := colVal.(type) {
		case string:
			v.AddVal(colSchema.ColumnName, t)
		case []byte:
			v.AddVal(colSchema.ColumnName, t)
		case time.Time: // mysql:date
			v.AddVal(colSchema.ColumnName, t.UTC().Unix()/86400)
		default:
			return xerrors.Errorf("unknown input data type for type string(yt:utf8): %T", colVal)
		}

	case string(ytschema.TypeBoolean):
		switch t := colVal.(type) {
		case bool:
			v.AddVal(colSchema.ColumnName, t)
		case int8: // mysql case - tinyint(1) not differs from boolean
			v.AddVal(colSchema.ColumnName, t == 1)
		default:
			return xerrors.Errorf("unknown input data type for type bool: %T", colVal)
		}

	case string(ytschema.TypeDatetime):
		switch t := colVal.(type) {
		case time.Time:
			v.AddVal(colSchema.ColumnName, t)
		default:
			return xerrors.Errorf("unknown input data type for type datetime: %T", colVal)
		}

	case string(ytschema.TypeTimestamp):
		switch t := colVal.(type) {
		case time.Time:
			v.AddVal(colSchema.ColumnName, t)
		default:
			return xerrors.Errorf("unknown input data type for type timestamp: %T", colVal)
		}

	case string(ytschema.TypeAny):
		switch t := colVal.(type) {
		case string:
			v.AddVal(colSchema.ColumnName, t)
		case map[string]interface{}:
			val, err := util.JSONMarshalUnescape(t)
			if err != nil {
				return xerrors.Errorf("unable to marshal object, err: %w, obj: %v", err, t)
			}
			v.AddVal(colSchema.ColumnName, string(val))
		default:
			return xerrors.Errorf("unknown input data type for type any: %T", colVal)
		}
	default:
		return xerrors.Errorf("unknown input data type: %s", colSchema.DataType)
	}

	return nil
}

var mapYtTypeToKafkaType = map[string]debeziumcommon.KafkaTypeDescr{
	string(ytschema.TypeInt64): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int64", "", nil
	}},
	string(ytschema.TypeInt32): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int32", "", nil
	}},
	string(ytschema.TypeInt16): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int16", "", nil
	}},
	string(ytschema.TypeInt8): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int8", "", nil
	}},

	string(ytschema.TypeUint64): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int64", "", nil
	}},
	string(ytschema.TypeUint32): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int32", "", nil
	}},
	string(ytschema.TypeUint16): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int16", "", nil
	}},
	string(ytschema.TypeUint8): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int8", "", nil
	}},

	string(ytschema.TypeFloat32): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "float", "", nil
	}},
	string(ytschema.TypeFloat64): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "double", "", nil
	}},

	string(ytschema.TypeBytes): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "bytes", "", nil
	}},
	string(ytschema.TypeString): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	string(ytschema.TypeBoolean): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "boolean", "", nil
	}},
	string(ytschema.TypeTimestamp): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "io.debezium.time.ZonedTimestamp", nil
	}},
	string(ytschema.TypeAny): {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
}

func colSchemaToOriginalType(colSchema *abstract.ColSchema) (*debeziumcommon.KafkaTypeDescr, error) {
	resultFunc, ok := mapYtTypeToKafkaType[colSchema.DataType]
	if !ok {
		return nil, xerrors.Errorf("unable to find yt type: %s", colSchema.DataType)
	}
	return &resultFunc, nil
}
