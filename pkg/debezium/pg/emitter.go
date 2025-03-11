package pg

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	"github.com/transferria/transferria/pkg/debezium/typeutil"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/util"
)

var mapPgNotParametrizedTypeToKafkaType = map[string]debeziumcommon.KafkaTypeDescr{
	"pg:boolean": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "boolean", "", nil
	}},
	"pg:bit(1)": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "boolean", "", nil
	}},
	"pg:smallint": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int16", "", nil
	}},
	"pg:integer": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int32", "", nil
	}},
	"pg:bigint": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int64", "", nil
	}},
	"pg:oid": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int64", "", nil
	}},
	"pg:real": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "float", "", nil
	}},
	"pg:double precision": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "double", "", nil
	}},
	"pg:bytea": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "bytes", "", nil
	}},
	"pg:json": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "io.debezium.data.Json", nil
	}},
	"pg:jsonb": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "io.debezium.data.Json", nil
	}},
	"pg:xml": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "io.debezium.data.Xml", nil
	}},
	"pg:uuid": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "io.debezium.data.Uuid", nil
	}},
	"pg:point": {KafkaTypeAndDebeziumNameAndExtra: pgPointExtra},
	"pg:inet": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:int4range": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:int8range": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:numrange": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:tsrange": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:tstzrange": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:daterange": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:text": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:date": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int32", "io.debezium.time.Date", nil
	}},
	"pg:money": {KafkaTypeAndDebeziumNameAndExtra: pgMoneyExtra},
	"pg:cidr": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:macaddr": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:character": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:character varying": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:hstore": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "io.debezium.data.Json", nil
	}},
	"pg:citext": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	//
}

var mapPgParametrizedTypePrefixToKafkaType = map[string]debeziumcommon.KafkaTypeDescr{
	"pg:bit(":         {KafkaTypeAndDebeziumNameAndExtra: typeutil.BitParametersExtractor},
	"pg:bit varying(": {KafkaTypeAndDebeziumNameAndExtra: typeutil.BitParametersExtractor},
	"pg:character(": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:character varying(": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "string", "", nil
	}},
	"pg:interval": {KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
		return "int64", "io.debezium.time.MicroDuration", nil
	}},
}

func pgMoneyExtra(_ *abstract.ColSchema, _, _ bool, connectorParameters map[string]string) (string, string, map[string]interface{}) {
	switch debeziumparameters.GetDecimalHandlingMode(connectorParameters) {
	case debeziumparameters.DecimalHandlingModePrecise:
		return "bytes", "org.apache.kafka.connect.data.Decimal", map[string]interface{}{"parameters": map[string]string{"scale": "2"}}
	case debeziumparameters.DecimalHandlingModeDouble:
		return "double", "", nil
	case debeziumparameters.DecimalHandlingModeString:
		return "string", "", nil
	default:
		return "", "", nil
	}
}

func pgNumericExtra(colSchema *abstract.ColSchema, _, _ bool, connectorParameters map[string]string) (string, string, map[string]interface{}) {
	switch debeziumparameters.GetDecimalHandlingMode(connectorParameters) {
	case debeziumparameters.DecimalHandlingModePrecise:
		result := make(map[string]interface{})
		putScaleToValue, precision, scale, _ := typeutil.DecimalGetPrecisionAndScale(typeutil.OriginalTypeWithoutProvider(colSchema.OriginalType))
		if putScaleToValue {
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
		} else {
			return typeutil.FieldDescrDecimal(precision, scale)
		}
	case debeziumparameters.DecimalHandlingModeDouble:
		return "double", "", nil
	case debeziumparameters.DecimalHandlingModeString:
		return "string", "", nil
	default:
		return "", "", nil
	}
}

func pgEnum(colSchema *abstract.ColSchema, _, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	arr := postgres.GetPropertyEnumAllValues(colSchema)

	result := make(map[string]interface{})
	result["version"] = 1
	result["parameters"] = map[string]string{"allowed": strings.Join(arr, ",")}

	return "string", "io.debezium.data.Enum", result
}

func convertFloatNanInf(val float64) interface{} {
	if math.IsNaN(val) {
		return "NaN"
	}
	if math.IsInf(val, -1) {
		return "-Infinity"
	}
	if math.IsInf(val, 1) {
		return "Infinity"
	}
	return val
}

func pgPointExtra(_ *abstract.ColSchema, _, _ bool, _ map[string]string) (string, string, map[string]interface{}) {
	result := make(map[string]interface{})
	result["doc"] = "Geometry (POINT)"
	fields := []map[string]interface{}{
		{
			"type":     "double",
			"optional": false,
			"field":    "x",
		},
		{
			"type":     "double",
			"optional": false,
			"field":    "y",
		},
		{
			"type":     "bytes",
			"optional": true,
			"field":    "wkb",
		},
		{
			"type":     "int32",
			"optional": true,
			"field":    "srid",
		},
	}
	result["fields"] = fields
	return "struct", "io.debezium.data.geometry.Point", result
}

func GetKafkaTypeDescrByPgType(colSchema *abstract.ColSchema) (*debeziumcommon.KafkaTypeDescr, error) {
	typeName := colSchema.OriginalType
	if val, ok := mapPgNotParametrizedTypeToKafkaType[typeName]; ok {
		return &val, nil
	} else {
		for k, v := range mapPgParametrizedTypePrefixToKafkaType {
			if strings.HasPrefix(typeName, k) {
				return &v, nil
			}
		}
		if postgres.IsPgTypeTimeWithTimeZone(typeName) {
			return &debeziumcommon.KafkaTypeDescr{KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
				return "string", "io.debezium.time.ZonedTime", nil
			}}, nil
		}
		if postgres.IsPgTypeTimeWithoutTimeZone(typeName) {
			return &debeziumcommon.KafkaTypeDescr{KafkaTypeAndDebeziumNameAndExtra: typeutil.TimePgWithoutTZParamsToKafkaType}, nil
		}
		if postgres.IsPgTypeTimestampWithTimeZone(typeName) {
			return &debeziumcommon.KafkaTypeDescr{KafkaTypeAndDebeziumNameAndExtra: func(*abstract.ColSchema, bool, bool, map[string]string) (string, string, map[string]interface{}) {
				return "string", "io.debezium.time.ZonedTimestamp", nil
			}}, nil
		}
		if postgres.IsPgTypeTimestampWithoutTimeZone(typeName) {
			return &debeziumcommon.KafkaTypeDescr{KafkaTypeAndDebeziumNameAndExtra: typeutil.TimestampPgParamsTypeToKafkaType}, nil
		}
		if typeutil.IsPgNumeric(typeName) {
			return &debeziumcommon.KafkaTypeDescr{KafkaTypeAndDebeziumNameAndExtra: pgNumericExtra}, nil
		}
		if postgres.GetPropertyEnumAllValues(colSchema) != nil {
			return &debeziumcommon.KafkaTypeDescr{KafkaTypeAndDebeziumNameAndExtra: pgEnum}, nil
		}
		return nil, debeziumcommon.NewUnknownTypeError(xerrors.Errorf("unknown pgType: %s", typeName))
	}
}

func GetOriginalTypeProperties(in *abstract.ColSchema) map[string]string {
	if val, ok := in.Properties[postgres.DatabaseTimeZone]; ok {
		return map[string]string{"timezone": val.(string)}
	}
	return nil
}

func AddPg(v *debeziumcommon.Values, colSchema *abstract.ColSchema, colName string, colVal interface{}, originalType string, intoArr bool, connectorParameters map[string]string) error {
	if colVal == nil {
		v.AddVal(colName, nil)
		return nil
	}

	switch originalType {
	case "pg:boolean":
		v.AddVal(colName, colVal)
	case "pg:bit(1)":
		v.AddVal(colName, colVal == "1")
	case "pg:smallint":
		switch t := colVal.(type) {
		case int16: // restored snapshot
			v.AddVal(colName, int(t))
		case int64: // original snapshot
			v.AddVal(colName, int(t))
		case json.Number: // into ARRAY
			val, err := t.Int64()
			if err != nil {
				return xerrors.Errorf("unable to get int64 from json.Number:%s", t.String())
			}
			v.AddVal(colName, int(val))
		case int: // received
			v.AddVal(colName, t)
		default:
			return xerrors.Errorf("unknown type of value for pg:smallint: %T", colVal)
		}
	case "pg:integer":
		switch t := colVal.(type) {
		case int32: // restored snapshot
			v.AddVal(colName, int(t))
		case int64: // original snapshot
			v.AddVal(colName, int(t))
		case int: // original replication
			v.AddVal(colName, t)
		case json.Number: // into ARRAY
			val, err := t.Int64()
			if err != nil {
				return xerrors.Errorf("unable to get int64 from json.Number:%s", t.String())
			}
			v.AddVal(colName, int(val))
		default:
			return xerrors.Errorf("unknown type of value for pg:integer: %T", colVal)
		}
	case "pg:oid":
		switch t := colVal.(type) {
		case int32:
			v.AddVal(colName, int64(t))
		case float64: // restored snapshot
			v.AddVal(colName, int64(t))
		case int64: // original snapshot
			v.AddVal(colName, t)
		case json.Number: // original replication
			val, err := t.Int64()
			if err != nil {
				return xerrors.Errorf("json.Number.Int64() returned an error: %w", err)
			}
			v.AddVal(colName, val)
		case uint32: // into ARRAY
			v.AddVal(colName, int64(t))
		default:
			return xerrors.Errorf("unknown type of value: %T", colVal)
		}
	case "pg:bigint":
		switch t := colVal.(type) {
		case int64: // original snapshot
			v.AddVal(colName, t)
		case json.Number: // original replication
			val, err := t.Int64()
			if err != nil {
				return xerrors.Errorf("json.Number.Int64() returned an error: %w", err)
			}
			v.AddVal(colName, val)
		default:
			return xerrors.Errorf("unknown type of value: %T", colVal)
		}
	case "pg:real":
		switch t := colVal.(type) {
		case float64: // original
			v.AddVal(colName, float32(t))
		case json.Number: // restored
			currFloat, err := t.Float64()
			if err != nil {
				return xerrors.Errorf("Float64 returned error, err: %w", err)
			}
			v.AddVal(colName, float32(currFloat))
		case float32: // into ARRAY
			v.AddVal(colName, t)
		default:
			return xerrors.Errorf("unknown type of value for 'pg:real': %T", colVal)
		}
	case "pg:double precision":
		switch t := colVal.(type) {
		case float64: // original
			v.AddVal(colName, convertFloatNanInf(t))
		case json.Number: // restored
			currFloat, err := t.Float64()
			if err != nil {
				return xerrors.Errorf("Float64 returned error, err: %w", err)
			}
			v.AddVal(colName, convertFloatNanInf(currFloat))
		default:
			return xerrors.Errorf("unknown type of value for 'pg:double precision': %T", colVal)
		}
	case "pg:bytea":
		val, err := typeutil.ParseBytea(colVal, v.ConnectorParameters[debeziumparameters.BinaryHandlingMode])
		if err != nil {
			return xerrors.Errorf("pg - bytea - unable to parse bytea, err: %w", err)
		}
		v.AddVal(colName, val)
	case "pg:json", "pg:jsonb":
		// we have here unmarshalled json! in interface{} located map[string]interface{}
		str, err := util.JSONMarshalUnescape(colVal)
		if err != nil {
			return xerrors.Errorf("pg - json - marshal returned error, err: %w", err)
		}
		v.AddVal(colName, string(str))
	case "pg:xml":
		v.AddVal(colName, typeutil.UnescapeUnicode(colVal.(string)))
	case "pg:uuid":
		switch t := colVal.(type) {
		case string:
			v.AddVal(colName, t)
		default:
			return xerrors.Errorf("unknown type of value for uuid: %T", colVal)
		}
	case "pg:point":
		mapJSON, err := typeutil.PointToDebezium(colVal.(string))
		if err != nil {
			return xerrors.Errorf("pg - point - convertor returned error, err: %w", err)
		}
		v.AddVal(colName, mapJSON)
	case "pg:inet":
		switch t := colVal.(type) {
		case string:
			result := strings.TrimSuffix(t, "/32")
			v.AddVal(colName, result)
		case map[string]interface{}: // into ARRAY
			v.AddVal(colName, t["IP"])
		default:
			return xerrors.Errorf("unknown type of value for inet: %T", colVal)
		}
	case "pg:int4range", "pg:int8range":
		v.AddVal(colName, colVal.(string))
	case "pg:numrange":
		val, err := typeutil.NumRangeToDebezium(colVal.(string))
		if err != nil {
			return xerrors.Errorf("pg - numrange - unable to convert numrange to debezium format, err: %w", err)
		}
		v.AddVal(colName, val)
	case "pg:tsrange":
		colStr := colVal.(string)
		leftBracket := string(colStr[0])
		rightBracket := string(colStr[len(colStr)-1])
		colStr = colStr[1 : len(colStr)-1]
		parts := strings.Split(colStr, ",")
		for i := range parts {
			parts[i] = "\"" + typeutil.UnquoteIfQuoted(parts[i]) + "\""
		}
		resultStr := strings.Join(parts, ",")
		v.AddVal(colName, leftBracket+resultStr+rightBracket)
	case "pg:tstzrange":
		colValStr := colVal.(string)
		result, err := typeutil.TstZRangeQuote(colValStr)
		if err != nil {
			return xerrors.Errorf("pg - tstzrange - unable to convert, string: %s, err: %w", colValStr, err)
		}
		v.AddVal(colName, result)
	case "pg:daterange":
		switch t := colVal.(type) {
		case []interface{}: // restored snapshot
			if len(t) != 2 {
				return xerrors.Errorf("len([]interface{}) != 2. len:%d", len(t))
			}
			left := t[0].(string)
			leftTime, err := time.Parse("2006-01-02T15:04:05Z", left)
			if err != nil {
				return xerrors.Errorf("pg - daterange - left time.Parse returned error, string: %s, err: %w", left, err)
			}
			right := t[1].(string)
			rightTime, err := time.Parse("2006-01-02T15:04:05Z", right)
			if err != nil {
				return xerrors.Errorf("pg - daterange - right time.Parse returned error, string: %s, err: %w", left, err)
			}
			v.AddVal(colName, "["+leftTime.UTC().Format("2006-01-02")+","+rightTime.UTC().Format("2006-01-02")+")")
		case []time.Time: // original snapshot
			if len(t) != 2 {
				return xerrors.Errorf("len([]time.Time) != 2. len:%d", len(t))
			}
			v.AddVal(colName, postgres.DaterangeToString(t))
		case string: // original replication
			v.AddVal(colName, t)
		default:
			return xerrors.Errorf("unknown type of value for daterange: %T", colVal)
		}
	case "pg:text":
		v.AddVal(colName, colVal.(string))
	case "pg:date":
		switch t := colVal.(type) {
		case string: // restored snapshot
			date, err := typeutil.ParsePgDateTimeWithTimezone(t)
			if err != nil {
				return xerrors.Errorf("unable to parse date, err: %w", err)
			}
			v.AddVal(colName, int(date.Unix()/(3600*24)))
		case time.Time: // original snapshot
			v.AddVal(colName, int(t.Unix()/(3600*24)))
		default:
			return xerrors.Errorf("unknown type of value for pg:date: %T", colVal)
		}
	case "pg:money":
		value, err := typeutil.DecimalToDebeziumPrimitives(colVal.(string)[1:], connectorParameters)
		if err != nil {
			return xerrors.Errorf("unable to extract debezium primitives from money, err: %w", err)
		}
		v.AddVal(colName, value)
	case "pg:cidr":
		v.AddVal(colName, colVal.(string))
	case "pg:macaddr":
		v.AddVal(colName, colVal.(string))
	case `pg:citext`:
		v.AddVal(colName, colVal.(string))
	case `pg:hstore`:
		result := ""
		var err error
		switch t := colVal.(type) {
		case map[string]interface{}:
			str, err := util.JSONMarshalUnescape(t)
			if err != nil {
				return xerrors.Errorf("pg - hstore - marshal returned error, err: %w", err)
			}
			result = string(str)
		case string:
			result, err = postgres.HstoreToJSON(t)
			if err != nil {
				return xerrors.Errorf("pg - hstore - hstoreToJSON returned error, hstore val: %s, err: %w", t, err)
			}
		default:
			return xerrors.Errorf("unknown type of value for pg:hstore: %T", colVal)
		}
		v.AddVal(colName, result)
		return nil
	default:
		if postgres.IsPgTypeTimeWithTimeZone(originalType) {
			val, casts := colVal.(string)
			if !casts {
				return xerrors.Errorf("pg - unable to process %s: expected string, got %T", originalType, colVal)
			}
			t, err := postgres.TimeWithTimeZoneToTime(val)
			if err != nil {
				return xerrors.Errorf("pg - failed to parse %s: %w", originalType, err)
			}
			t = t.UTC()
			tFormat := "15:04:05.999999Z"
			if intoArr {
				// debezium-specific behaviour, decreasing accuracy
				tFormat = "15:04:05Z"
			}
			v.AddVal(colName, t.Format(tFormat))
			return nil
		} else if postgres.IsPgTypeTimeWithoutTimeZone(originalType) {
			t := new(pgtype.Time)
			if err := t.Scan(colVal); err != nil {
				return xerrors.Errorf("pg - unable to parse %s %v: %w", originalType, colVal, err)
			}
			if t.Status != pgtype.Present {
				return xerrors.Errorf("pg - unable to parse %s %v: parsed to nil", originalType, colVal)
			}
			divider, err := typeutil.GetTimeDivider(typeutil.OriginalTypeWithoutProvider(originalType))
			if err != nil {
				return xerrors.Errorf("pg - unable to get time precision for %s: %w", originalType, err)
			}
			if intoArr {
				// debezium-specific behaviour
				divider = 1
			}
			result := t.Microseconds / int64(divider)
			if intoArr {
				// debezium-specific behaviour, decreasing accuracy
				result = result - (result % 1000)
			}
			v.AddVal(colName, result)
			return nil
		} else if postgres.IsPgTypeTimestampWithoutTimeZone(originalType) {
			ts := new(pgtype.Timestamp)
			if err := ts.Set(colVal); err != nil {
				return xerrors.Errorf("pg - unable to parse %s %v: %w", originalType, colVal, err)
			}
			if ts.Status != pgtype.Present {
				return xerrors.Errorf("pg - unable to parse %s %v: parsed to nil", originalType, colVal)
			}
			if ts.InfinityModifier != pgtype.None {
				return xerrors.Errorf("pg - unable to parse %s %v: parsed to infinity timestamp", originalType, colVal)
			}
			divider, err := typeutil.GetTimeDivider(typeutil.OriginalTypeWithoutProvider(originalType))
			if err != nil {
				return xerrors.Errorf("pg - unable to get time precision for %s: %w", originalType, err)
			}
			if intoArr {
				divider = 1
			}
			result := ts.Time.UnixMicro() / int64(divider)
			v.AddVal(colName, result)
			return nil
		} else if postgres.IsPgTypeTimestampWithTimeZone(originalType) {
			switch t := colVal.(type) {
			case time.Time: // original snapshot
				v.AddVal(colName, typeutil.SprintfDebeziumTime(t))
			case string: // restored snapshot
				currTime, err := typeutil.ParsePgDateTimeWithTimezone(t)
				if err != nil {
					return xerrors.Errorf("parsePgDateTimeWithTimezone returned error, err: %w", err)
				}
				v.AddVal(colName, typeutil.SprintfDebeziumTime(currTime))
			default:
				return xerrors.Errorf("unknown type of value for 'pg:timestamp with time zone': %T", colVal)
			}
		} else if typeutil.IsPgNumeric(originalType) {
			var val string
			var err error
			switch t := colVal.(type) {
			case string:
				val = t
			case json.Number:
				val = t.String()
			case map[string]interface{}: // into ARRAY
				val = t["Int"].(json.Number).String()
				expStr := t["Exp"].(json.Number).String()
				exp, _ := strconv.Atoi(expStr)
				if exp != 0 {
					l := len(val)
					val = val[:l+exp] + "." + val[l+exp:]
				}
			default:
				return xerrors.Errorf("unknown type of value for 'pg:numeric': %T", colVal)
			}
			result, err := typeutil.DecimalToDebezium(val, typeutil.OriginalTypeWithoutProvider(originalType), connectorParameters)
			if err != nil {
				return xerrors.Errorf("pg - unable to build numeric value, err: %w", err)
			}
			v.AddVal(colName, result)
			return nil
		} else if strings.HasPrefix(originalType, "pg:bit(") || strings.HasPrefix(originalType, "pg:bit varying(") {
			v.AddVal(colName, typeutil.ChangeItemsBitsToDebeziumHonest(colVal.(string)))
			return nil
		} else if strings.HasPrefix(originalType, "pg:character(") || strings.HasPrefix(originalType, "pg:character varying(") || originalType == "pg:character" || originalType == "pg:character varying" { // strong equality - into ARRAY
			v.AddVal(colName, colVal.(string))
			return nil
		} else if strings.HasPrefix(originalType, "pg:interval") {
			val, err := typeutil.ParsePostgresInterval(colVal.(string), v.ConnectorParameters[debeziumparameters.IntervalHandlingMode])
			if err != nil {
				return xerrors.Errorf("pg - interval - unknown interval: %s, err: %w", colVal.(string), err)
			}
			v.AddVal(colName, val)
			return nil
		} else if postgres.GetPropertyEnumAllValues(colSchema) != nil {
			v.AddVal(colName, colVal.(string))
			return nil
		} else {
			return debeziumcommon.NewUnknownTypeError(xerrors.Errorf("unknown column type: %s, column name: %s", originalType, colName))
		}
	}
	return nil
}
