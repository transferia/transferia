package mysql

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/debezium/typeutil"
	"github.com/transferia/transferia/pkg/util"
)

var mapMysqlNotParametrizedTypeToKafkaType = map[string]debeziumcommon.KafkaTypeDescr{
	"mysql:blob": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "bytes", "", nil
	}},
	"mysql:date": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int32", "io.debezium.time.Date", nil
	}},
	"mysql:float": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "double", "", nil
	}},
	"mysql:json": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "io.debezium.data.Json", nil
	}},
	"mysql:longblob": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "bytes", "", nil
	}},
	"mysql:longtext": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"mysql:mediumblob": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "bytes", "", nil
	}},
	"mysql:mediumtext": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"mysql:text": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"mysql:tinyblob": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "bytes", "", nil
	}},
	"mysql:tinytext": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"mysql:time": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int64", "io.debezium.time.MicroTime", nil
	}},
}

var mapMysqlParametrizedTypePrefixToKafkaType = map[string]debeziumcommon.KafkaTypeDescr{
	"mysql:bigint": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int64", "", nil
	}},
	"mysql:binary(": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "bytes", "", nil
	}},
	"mysql:bit(": {KafkaTypeAndDebeziumNameAndExtra: typeutil.BitParametersExtractor},
	"mysql:char(": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"mysql:datetime": {KafkaTypeAndDebeziumNameAndExtra: typeutil.TimestampMysqlParamsTypeToKafkaType},
	"mysql:decimal(": {KafkaTypeAndDebeziumNameAndExtra: typeutil.MysqlDecimalFieldDescr},
	"mysql:double": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "double", "", nil
	}},
	"mysql:enum(": {KafkaTypeAndDebeziumNameAndExtra: enumParamsToKafkaType},
	"mysql:int":   {KafkaTypeAndDebeziumNameAndExtra: integerParamsToKafkaType},
	"mysql:mediumint": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int32", "", nil
	}},
	"mysql:set(":     {KafkaTypeAndDebeziumNameAndExtra: setParamsToKafkaType},
	"mysql:smallint": {KafkaTypeAndDebeziumNameAndExtra: smallintParamsToKafkaType},
	"mysql:time(": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int64", "io.debezium.time.MicroTime", nil
	}},
	"mysql:timestamp": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "io.debezium.time.ZonedTimestamp", nil
	}},
	"mysql:tinyint": {KafkaTypeAndDebeziumNameAndExtra: tinyintParamsToKafkaType},
	"mysql:varbinary(": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "bytes", "", nil
	}},
	"mysql:varchar(": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"mysql:year": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int32", "io.debezium.time.Year", nil
	}},
}

func tinyintParamsToKafkaType(colSchema *abstract.ColSchema, _, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	// ------------------------------------------------------------------------------------------------------------------------------------------------------
	// MySQL handles the BOOLEAN value internally in a specific way.
	// The BOOLEAN column is internally mapped to the TINYINT(1) data type.
	// When the table is created during streaming then it uses proper BOOLEAN mapping as Debezium receives the original DDL.
	// During snapshots, Debezium executes SHOW CREATE TABLE to obtain table definitions that return TINYINT(1) for both BOOLEAN and TINYINT(1) columns.
	// Debezium then has no way to obtain the original type mapping and so maps to TINYINT(1).
	//
	// Data-transfer can't divide tinyint(1) from bool - always will be bool!
	// ------------------------------------------------------------------------------------------------------------------------------------------------------
	if colSchema.OriginalType == "mysql:tinyint(1)" {
		return "boolean", "", nil
	} else {
		return "int16", "", nil
	}
}

func smallintParamsToKafkaType(colSchema *abstract.ColSchema, _, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	if strings.HasSuffix(colSchema.OriginalType, " unsigned") {
		return "int32", "", nil
	} else {
		return "int16", "", nil
	}
}

func integerParamsToKafkaType(colSchema *abstract.ColSchema, _, isSnapshot bool, _ map[string]string) (string, string, map[string]interface{}) {
	enableInt64 := true
	if !isSnapshot && colSchema.PrimaryKey {
		enableInt64 = false
	}
	if strings.HasSuffix(colSchema.OriginalType, " unsigned") && enableInt64 {
		return "int64", "", nil
	} else {
		return "int32", "", nil
	}
}

func enumParamsToKafkaType(colSchema *abstract.ColSchema, _, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	allowedRaw := colSchema.OriginalType[11 : len(colSchema.OriginalType)-1]
	allowed, _ := typeutil.UnwrapMysqlEnumsAndSets(allowedRaw)
	result := map[string]interface{}{
		"parameters": map[string]interface{}{
			"allowed": allowed,
		},
	}
	return "string", "io.debezium.data.Enum", result
}

func setParamsToKafkaType(colSchema *abstract.ColSchema, _, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	allowedRaw := colSchema.OriginalType[10 : len(colSchema.OriginalType)-1]
	allowed, _ := typeutil.UnwrapMysqlEnumsAndSets(allowedRaw)
	result := map[string]interface{}{
		"parameters": map[string]interface{}{
			"allowed": allowed,
		},
	}
	return "string", "io.debezium.data.EnumSet", result
}

func GetKafkaTypeDescrByMysqlType(typeName string) (*debeziumcommon.KafkaTypeDescr, error) {
	if val, ok := mapMysqlNotParametrizedTypeToKafkaType[typeName]; ok {
		return &val, nil
	} else {
		for k, v := range mapMysqlParametrizedTypePrefixToKafkaType {
			if strings.HasPrefix(typeName, k) {
				return &v, nil
			}
		}
		return nil, xerrors.Errorf("unknown mysqlType: %s", typeName)
	}
}

func AddMysql(v *debeziumcommon.Values, colName string, colVal interface{}, colType string, _ bool, connectorParameters map[string]string) error {
	if colVal == nil {
		v.AddVal(colName, nil)
		return nil
	}

	unparametrizedOriginalType := abstract.TrimMySQLType(colType)
	switch unparametrizedOriginalType {
	case "mysql:int":
		switch t := colVal.(type) {
		case int32: // restored snapshot
			v.AddVal(colName, t)
		case uint32: // restored snapshot, int unsigned
			v.AddVal(colName, t)
		default:
			return xerrors.Errorf("unknown type of value for mysql:int: %T", colVal)
		}
	case "mysql:bigint":
		switch t := colVal.(type) {
		case int64: // restored snapshot, bigint
			v.AddVal(colName, t)
		case uint64: // restored snapshot, bigint unsigned
			v.AddVal(colName, int64(t))
		default:
			return xerrors.Errorf("unknown type of value for mysql:bigint: %T", colVal)
		}
	case "mysql:binary", "mysql:varbinary", "mysql:longblob", "mysql:mediumblob", "mysql:blob", "mysql:tinyblob":
		currVal := colVal
		if !strings.HasPrefix(colType, "mysql:varbinary") {
			var err error
			currVal, err = typeutil.MysqlFitBinaryLength(colType, currVal)
			if err != nil {
				return xerrors.Errorf("mysql - binary - unable to fit binary length, err: %w", err)
			}
		}
		val, err := typeutil.ParseBytea(currVal, v.ConnectorParameters[debeziumparameters.BinaryHandlingMode])
		if err != nil {
			return xerrors.Errorf("mysql - binary - unable to parse binary/varbinary/longblob, err: %w", err)
		}
		v.AddVal(colName, val)
	case "mysql:bit":
		if colType == "mysql:bit(1)" {
			switch t := colVal.(type) {
			case string:
				// "AQ==" - snapshot restored
				// "AAAAAAAAAAE=" - replication restored
				v.AddVal(colName, (t == "AQ==") || (t == "AAAAAAAAAAE="))
			case []uint8:
				if len(t) == 8 { // original replication
					v.AddVal(colName, t[7] == 1)
				} else {
					if len(t) != 1 {
						return xerrors.Errorf("type mysql:bit has len(t) != 1, len: %d", len(t))
					}
					v.AddVal(colName, t[0] == 1) // original snapshot
				}
			}
		} else {
			currVal := colVal
			switch t := colVal.(type) {
			case string: // restored replication
				buf, err := base64.StdEncoding.DecodeString(t)
				if err != nil {
					return xerrors.Errorf("unable to decode mysql:bit base64, string: %s, err: %w", t, err)
				}
				currVal = buf
			}
			switch currVal.(type) {
			case []uint8: // original replication
				var err error
				currVal, err = typeutil.ShrinkMysqlBit(currVal, colType)
				if err != nil {
					return xerrors.Errorf("mysql - bit - unable to shrink buf, err: %w", err)
				}
			}
			val, err := typeutil.ParseMysqlBit(currVal, v.ConnectorParameters[debeziumparameters.BinaryHandlingMode])
			if err != nil {
				return xerrors.Errorf("mysql - bit - unable to parse bit, err: %w", err)
			}
			v.AddVal(colName, val)
		}
	case "mysql:float":
		switch t := colVal.(type) {
		case json.Number: // restored
			currFloat, err := t.Float64()
			if err != nil {
				return xerrors.Errorf("Float64 returned error, err: %w", err)
			}
			v.AddVal(colName, currFloat)
		case float64: // original snapshot
			v.AddVal(colName, t)
		case float32: // original replication
			v.AddVal(colName, float64(t))
		default:
			return xerrors.Errorf("unknown type of value for mysql:float: %T", colVal)
		}
	case "mysql:double":
		switch t := colVal.(type) {
		case json.Number: // restored
			currDouble, err := t.Float64()
			if err != nil {
				return xerrors.Errorf("Float64 returned error, err: %w", err)
			}
			v.AddVal(colName, currDouble)
		case float64: // original snapshot
			v.AddVal(colName, t)
		default:
			return xerrors.Errorf("unknown type of value for mysql:double: %T", colVal)
		}
	case "mysql:tinyint":
		if colType == "mysql:tinyint(1)" {
			// -------------------------------------------------------------------------------------------------------------------------------------------------
			// MySQL handles the BOOLEAN value internally in a specific way.
			// The BOOLEAN column is internally mapped to the TINYINT(1) data type.
			// When the table is created during streaming then it uses proper BOOLEAN mapping as Debezium receives the original DDL.
			// During snapshots, Debezium executes SHOW CREATE TABLE to obtain table definitions that return TINYINT(1) for both BOOLEAN and TINYINT(1) columns.
			// Debezium then has no way to obtain the original type mapping and so maps to TINYINT(1).
			//
			// Data-transfer can't divide tinyint(1) from bool - always will be bool!
			// -------------------------------------------------------------------------------------------------------------------------------------------------
			v.AddVal(colName, colVal.(int8) == 1)
		} else {
			switch t := colVal.(type) {
			case int8: // restored snapshot
				v.AddVal(colName, t)
			case uint8: // restored snapshot
				v.AddVal(colName, t)
			default:
				return xerrors.Errorf("unknown type of value for mysql:tinyint: %T", colVal)
			}
		}
	case "mysql:char", "mysql:varchar", "mysql:longtext", "mysql:mediumtext", "mysql:text", "mysql:tinytext":
		v.AddVal(colName, colVal.(string))
	case "mysql:mediumint":
		switch t := colVal.(type) {
		case int32: // restored snapshot
			v.AddVal(colName, t)
		case uint32: // restored snapshot
			v.AddVal(colName, t)
		default:
			return xerrors.Errorf("unknown type of value for mysql:mediumint: %T", colVal)
		}
	case "mysql:smallint":
		switch t := colVal.(type) {
		case int16: // restored snapshot
			v.AddVal(colName, t)
		case uint16: // restored snapshot
			v.AddVal(colName, t)
		default:
			return xerrors.Errorf("unknown type of value for mysql:smallint: %T", colVal)
		}
	case "mysql:json":
		marshaledJSON, err := util.JSONMarshalUnescape(colVal)
		if err != nil {
			return xerrors.Errorf("cannot marshal JSON: %w", err)
		}
		v.AddVal(colName, string(marshaledJSON))
	case "mysql:timestamp":
		timeVal := colVal.(time.Time).UTC()
		var precision int
		if colType == "mysql:timestamp" {
			precision = 0
		} else {
			precision = typeutil.GetTimePrecision(colType)
		}
		v.AddVal(colName, typeutil.FormatTime(timeVal, precision))
	case "mysql:datetime":
		var timeDivider int
		if colType == "mysql:datetime" {
			timeDivider = 1000
		} else {
			datetimePrecision := typeutil.GetTimePrecision(colType)
			if datetimePrecision > 3 {
				timeDivider = 1000000
			} else {
				timeDivider = 1000
			}
		}
		currTime := colVal.(time.Time)
		v.AddVal(colName, uint64(currTime.Unix())*uint64(timeDivider)+uint64(currTime.Nanosecond()/(1000000000/timeDivider)))
	case "mysql:date":
		timeVal := colVal.(time.Time).UTC()
		v.AddVal(colName, timeVal.Unix()/86400)
	case "mysql:time":
		timeVal, err := typeutil.ParseTimeWithoutTZ(colVal.(string))
		if err != nil {
			return xerrors.Errorf("unable to parse time, time: %s, err: %w", colVal, err)
		}
		h, m, s := timeVal.Clock()
		result := uint64(h*3600+60*m+s)*1000000 + uint64(timeVal.Nanosecond())/1000
		v.AddVal(colName, result)
	case "mysql:set":
		v.AddVal(colName, colVal.(string))
	case "mysql:enum":
		v.AddVal(colName, colVal.(string))
	case "mysql:decimal":
		decimalValStr := ""
		switch t := colVal.(type) {
		case json.Number:
			decimalValStr = t.String()
		}
		value, err := typeutil.DecimalToDebeziumPrimitives(decimalValStr, connectorParameters)
		if err != nil {
			return xerrors.Errorf("unable to extract debezium primitives from mysql:decimal, err: %w", err)
		}
		v.AddVal(colName, value)
	case "mysql:year":
		year, err := strconv.Atoi(colVal.(string))
		if err != nil {
			return xerrors.Errorf("unable to extract debezium primitives from mysql:decimal, err: %w", err)
		}
		v.AddVal(colName, year)
	case "mysql:geometry":
		return debeziumcommon.NewUnknownTypeError(xerrors.Errorf("mysql:geometry type is not supported for now, column type: %s, column name: %s", colType, colName))
	default:
		return debeziumcommon.NewUnknownTypeError(xerrors.Errorf("unknown column type: %s, column name: %s", colType, colName))
	}
	return nil
}
