package ydb

import (
	"encoding/json"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/debezium/typeutil"
	"github.com/transferia/transferia/pkg/util"
)

var mapYDBTypeToKafkaType = map[string]*debeziumcommon.KafkaTypeDescr{
	"ydb:Bool": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeBoolean), "", nil
	}},

	"ydb:Int8": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt8), "", nil
	}},
	"ydb:Int16": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt16), "", nil
	}},
	"ydb:Int32": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt32), "", nil
	}},
	"ydb:Int64": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt64), "", nil
	}},
	"ydb:Uint8": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt8), "", nil
	}},
	"ydb:Uint16": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt16), "", nil
	}},
	"ydb:Uint32": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt32), "", nil
	}},
	"ydb:Uint64": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt64), "", nil
	}},

	"ydb:Float": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeFloat32), "", nil
	}},
	"ydb:Double": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeFloat64), "", nil
	}},
	"ydb:DyNumber": {KafkaTypeAndDebeziumNameAndExtra: ydbDyNumberExtra},
	"ydb:String": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeBytes), "", nil
	}},
	"ydb:Utf8": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeString), "", nil
	}},
	"ydb:Json": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeString), "io.debezium.data.Json", nil
	}},
	"ydb:JsonDocument": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeString), "io.debezium.data.Json", nil
	}},
	"ydb:Uuid": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeString), "", nil
	}},
	"ydb:Date": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt32), "io.debezium.time.Date", nil
	}},
	"ydb:Datetime": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt64), "io.debezium.time.Timestamp", nil
	}},
	"ydb:Timestamp": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt64), "io.debezium.time.MicroTimestamp", nil
	}},
	"ydb:Interval": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return string(debeziumcommon.KafkaTypeInt64), "io.debezium.time.MicroDuration", nil
	}},
}

func decimalExtra(colSchema *abstract.ColSchema, _, _ bool, connectorParameters map[string]string) (string, string, map[string]interface{}) {
	switch debeziumparameters.GetDecimalHandlingMode(connectorParameters) {
	case debeziumparameters.DecimalHandlingModePrecise:
		return typeutil.FieldDescrDecimal(22, 9)
	case debeziumparameters.DecimalHandlingModeDouble:
		return "double", "", nil
	case debeziumparameters.DecimalHandlingModeString:
		return "string", "", nil
	default:
		return "", "", nil
	}
}

func ydbDyNumberExtra(_ *abstract.ColSchema, _, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	result := make(map[string]interface{})
	result["doc"] = "Variable scaled decimal"
	fields := []map[string]interface{}{
		{
			"type":     "int32",
			"optional": false,
			"field":    "scale",
		},
		{
			"type":     "bytes",
			"optional": false,
			"field":    "value",
		},
	}
	result["fields"] = fields
	return "struct", "io.debezium.data.VariableScaleDecimal", result
}

func GetKafkaTypeDescrByYDBType(typeName string) (*debeziumcommon.KafkaTypeDescr, error) {
	if typeName == "ydb:Decimal" {
		return &debeziumcommon.KafkaTypeDescr{KafkaTypeAndDebeziumNameAndExtra: decimalExtra}, nil
	}
	typeDescr, ok := mapYDBTypeToKafkaType[typeName]
	if !ok {
		return nil, debeziumcommon.NewUnknownTypeError(xerrors.Errorf("unknown ydbType: %s", typeName))
	}
	return typeDescr, nil
}

func AddYDB(v *debeziumcommon.Values, colName string, colVal interface{}, colType string, connectorParameters map[string]string) error {
	if colVal == nil {
		v.AddVal(colName, nil)
		return nil
	}

	switch colType {
	case "ydb:Bool":
		v.AddVal(colName, colVal)

	case "ydb:Int8":
		v.AddVal(colName, colVal)
	case "ydb:Int16":
		v.AddVal(colName, colVal)
	case "ydb:Int32":
		v.AddVal(colName, colVal)
	case "ydb:Int64":
		v.AddVal(colName, colVal)

	case "ydb:Uint8":
		v.AddVal(colName, colVal)
	case "ydb:Uint16":
		v.AddVal(colName, colVal)
	case "ydb:Uint32":
		v.AddVal(colName, colVal)
	case "ydb:Uint64":
		switch t := colVal.(type) {
		case uint64:
			v.AddVal(colName, int64(t))
		default:
			return xerrors.Errorf("unknown type of value for ydb:Uint64: %T", colVal)
		}

	case "ydb:Float":
		v.AddVal(colName, colVal)
	case "ydb:Double":
		v.AddVal(colName, colVal)
	case "ydb:Decimal":
		result, err := typeutil.DecimalToDebezium(colVal.(string), "numeric(22,9)", connectorParameters)
		if err != nil {
			return xerrors.Errorf("ydb - unable to build numeric(22,9) value, err: %w", err)
		}
		v.AddVal(colName, result)
	case "ydb:DyNumber":
		var unpVal string
		switch vv := colVal.(type) {
		case string:
			unpVal = vv
		case json.Number:
			unpVal = vv.String()
		default:
			return xerrors.Errorf("unknown type of value for ydb:DyNumber: %T", colVal)
		}
		result, err := typeutil.DecimalToDebeziumHandlingModePrecise(unpVal, "numeric")
		if err != nil {
			return xerrors.Errorf("ydb - unable to build numeric value, err: %w", err)
		}
		v.AddVal(colName, result)

	case "ydb:String":
		v.AddVal(colName, colVal)
	case "ydb:Utf8":
		v.AddVal(colName, colVal)
	case "ydb:Json":
		// we have here unmarshalled json! in interface{} located map[string]interface{}
		str, err := util.JSONMarshalUnescape(colVal)
		if err != nil {
			return xerrors.Errorf("ydb - Json - marshal returned error, err: %w", err)
		}
		v.AddVal(colName, string(str))
	case "ydb:JsonDocument":
		// we have here unmarshalled json! in interface{} located map[string]interface{}
		str, err := util.JSONMarshalUnescape(colVal)
		if err != nil {
			return xerrors.Errorf("ydb - JsonDocument - marshal returned error, err: %w", err)
		}
		v.AddVal(colName, string(str))
	case "ydb:Uuid":
		v.AddVal(colName, colVal.(string))

	case "ydb:Date": //
		switch vv := colVal.(type) {
		case time.Time:
			v.AddVal(colName, typeutil.DateToInt32(vv))
		default:
			return xerrors.Errorf("impossible type %s(%s): %T expect time.Time", colName, colType, colVal)
		}
	case "ydb:Datetime":
		switch vv := colVal.(type) {
		case time.Time:
			v.AddVal(colName, typeutil.DatetimeToSecs(vv)) // this is govnocode, todo normalno here - TM-3968
		default:
			return xerrors.Errorf("impossible type %s(%s): %T expect time.Time", colName, colType, colVal)
		}
	case "ydb:Timestamp":
		switch vv := colVal.(type) {
		case time.Time:
			v.AddVal(colName, typeutil.DatetimeToMicrosecs(vv)) // this is govnocode, todo normalno here - TM-3968
		default:
			return xerrors.Errorf("impossible type %s(%s): %T expect time.Time", colName, colType, colVal)
		}
	case "ydb:Interval":
		v.AddVal(colName, colVal)

	default:
		return debeziumcommon.NewUnknownTypeError(xerrors.Errorf("unknown column type: %s, column name: %s", colType, colName))
	}
	return nil
}
