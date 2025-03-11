package pg

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	"github.com/transferria/transferria/pkg/debezium/typeutil"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/util/jsonx"
	"github.com/transferria/transferria/pkg/util/xlocale"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

//---------------------------------------------------------------------------------------------------------------------
// pg non-default converting

var KafkaTypeToOriginalTypeToFieldReceiverFunc = map[debeziumcommon.KafkaType]map[string]debeziumcommon.FieldReceiver{
	debeziumcommon.KafkaTypeInt32: {
		"pg:date": new(Date),
		debeziumcommon.DTMatchByFunc: &debeziumcommon.FieldReceiverMatchers{
			Matchers: []debeziumcommon.FieldReceiverMatcher{new(TimeWithoutTimeZone)},
		},
	},
	debeziumcommon.KafkaTypeInt64: {
		"pg:interval": new(Interval),
		"pg:oid":      new(Oid),
		debeziumcommon.DTMatchByFunc: &debeziumcommon.FieldReceiverMatchers{
			Matchers: []debeziumcommon.FieldReceiverMatcher{new(TimestampWithoutTimeZone), new(TimeWithoutTimeZone2)},
		},
	},
	debeziumcommon.KafkaTypeBoolean: {
		"pg:bit(1)": new(Bit1),
	},
	debeziumcommon.KafkaTypeBytes: {
		"pg:bit":         new(BitN),
		"pg:bit varying": new(BitVarying),
		"pg:money":       new(debeziumcommon.Decimal),
		debeziumcommon.DTMatchByFunc: &debeziumcommon.FieldReceiverMatchers{
			Matchers: []debeziumcommon.FieldReceiverMatcher{new(Decimal), new(DebeziumBuf)},
		},
	},
	debeziumcommon.KafkaTypeFloat64: {
		"pg:double precision": new(DoublePrecision),
	},
	debeziumcommon.KafkaTypeString: {
		"pg:inet":      new(Inet),
		"pg:int4range": new(debeziumcommon.StringToAnyDefault),
		"pg:int8range": new(debeziumcommon.StringToAnyDefault),
		"pg:json":      new(JSON),
		"pg:jsonb":     new(JSON),
		"pg:numrange":  new(NumRange),
		"pg:tsrange":   new(TSRange),
		"pg:tstzrange": new(TSTZRange),
		"pg:xml":       new(debeziumcommon.StringToAnyDefault),
		"pg:citext":    new(CIText),
		"pg:hstore":    new(HStore),
		"pg:macaddr":   new(StringButYTAny),
		"pg:cidr":      new(StringButYTAny),
		"pg:character": new(StringButYTAny),
		"pg:daterange": new(StringButYTAny),
		debeziumcommon.DTMatchByFunc: &debeziumcommon.FieldReceiverMatchers{
			Matchers: []debeziumcommon.FieldReceiverMatcher{new(TimestampWithTimeZone), new(Enum)},
		},
	},
	debeziumcommon.KafkaTypeStruct: {
		"pg:point": new(Point),
	},
}

//---------------------------------------------------------------------------------------------------------------------
// int32

type Date struct {
	debeziumcommon.Int64ToTime
	debeziumcommon.YTTypeDate
	debeziumcommon.FieldReceiverMarker
}

func (d *Date) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (time.Time, error) {
	return time.Unix(in*3600*24, 0).UTC(), nil
}

type TimeWithoutTimeZone struct {
	debeziumcommon.IntToString
	debeziumcommon.YTTypeString
	debeziumcommon.FieldReceiverMarker
}

func (t *TimeWithoutTimeZone) IsMatched(originalType *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return postgres.IsPgTypeTimeWithoutTimeZone(originalType.OriginalType)
}

func (t *TimeWithoutTimeZone) Do(in int64, originalType *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (string, error) {
	precision := typeutil.PgTimeWithoutTimeZonePrecision(originalType.OriginalType)
	if precision == 0 {
		valSec := in / 1000
		return fmt.Sprintf("%02d:%02d:%02d", valSec/3600, (valSec/60)%60, valSec%60), nil
	} else {
		if precision <= 3 {
			valSec := in / 1000
			result := fmt.Sprintf("%02d:%02d:%02d.%03d000", valSec/3600, (valSec/60)%60, valSec%60, in%1000)
			return result[0 : 9+precision], nil
		} else {
			valSec := in / 1000000
			result := fmt.Sprintf("%02d:%02d:%02d.%06d", valSec/3600, (valSec/60)%60, valSec%60, in%1000000)
			return result[0 : 9+precision], nil
		}
	}
}

// int64

type Interval struct {
	debeziumcommon.IntToString
	debeziumcommon.YTTypeString
	debeziumcommon.FieldReceiverMarker
}

func (i *Interval) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (string, error) {
	return typeutil.EmitPostgresInterval(in), nil
}

type TimestampWithoutTimeZone struct {
	debeziumcommon.Int64ToTime
	debeziumcommon.YTTypeTimestamp
	debeziumcommon.FieldReceiverMarker
}

func (t *TimestampWithoutTimeZone) IsMatched(originalType *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return postgres.IsPgTypeTimestampWithoutTimeZone(originalType.OriginalType)
}

var OriginalTypePropertyTimeZone = "timezone"

func (t *TimestampWithoutTimeZone) Do(in int64, originalType *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, intoArr bool) (time.Time, error) {
	var datetimePrecisionInt int
	if intoArr {
		datetimePrecisionInt = 6
	} else {
		if originalType.OriginalType == "pg:timestamp without time zone" {
			datetimePrecisionInt = 6
		} else {
			datetimePrecisionInt = typeutil.GetTimePrecision(typeutil.OriginalTypeWithoutProvider(originalType.OriginalType))
		}
	}
	var timestamp time.Time
	if datetimePrecisionInt >= 4 && datetimePrecisionInt <= 6 {
		timestamp = time.Unix(in/1000000, (in%1000000)*1000).UTC()
	} else {
		timestamp = time.Unix(in/1000, (in%1000)*1000000).UTC()
	}
	if originalType.Properties == nil {
		return timestamp, nil
	}
	timeZone := originalType.Properties[OriginalTypePropertyTimeZone]
	if timeZone == "" {
		return timestamp, nil
	}
	tz, err := xlocale.Load(timeZone)
	if err != nil {
		return time.Time{}, xerrors.Errorf("unable to load timezone %s, err: %w", timeZone, err)
	}
	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), timestamp.Second(), timestamp.Nanosecond(), tz), nil
}

type TimeWithoutTimeZone2 struct {
	debeziumcommon.IntToString
	debeziumcommon.YTTypeString
	debeziumcommon.FieldReceiverMarker
}

func (t *TimeWithoutTimeZone2) IsMatched(originalType *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return postgres.IsPgTypeTimeWithoutTimeZone(originalType.OriginalType)
}

func (t *TimeWithoutTimeZone2) Do(in int64, originalType *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, intoArr bool) (string, error) {
	var precision int
	if intoArr {
		precision = 6
	} else {
		precision = typeutil.PgTimeWithoutTimeZonePrecision(originalType.OriginalType)
	}
	if precision == 0 {
		valSec := in / 1000000
		return fmt.Sprintf("%02d:%02d:%02d", valSec/3600, (valSec/60)%60, valSec%60), nil
	} else {
		if precision <= 3 {
			valSec := in / 1000
			result := fmt.Sprintf("%02d:%02d:%02d.%03d000", valSec/3600, (valSec/60)%60, valSec%60, in%1000)
			return result[0 : 9+precision], nil
		} else {
			valSec := in / 1000000
			result := fmt.Sprintf("%02d:%02d:%02d.%06d", valSec/3600, (valSec/60)%60, valSec%60, in%1000000)
			return result[0 : 9+precision], nil
		}
	}
}

// boolean

type Bit1 struct {
	debeziumcommon.IntToString
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (b *Bit1) Do(in bool, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (string, error) {
	if in {
		return "1", nil
	} else {
		return "0", nil
	}
}

// string

type Inet struct {
	debeziumcommon.StringToString
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (i *Inet) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (string, error) {
	if !strings.Contains(in, "/") {
		in += "/32"
	}
	return in, nil
}

type JSON struct {
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (i *JSON) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (interface{}, error) {
	var result interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&result); err != nil {
		return "", err
	}
	return result, nil
}

type DoublePrecision struct {
	debeziumcommon.AnyToDouble
	debeziumcommon.YTTypeFloat64
	debeziumcommon.FieldReceiverMarker
}

func (p *DoublePrecision) Do(in interface{}, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (float64, error) {
	switch val := in.(type) {
	case json.Number:
		vall, err := val.Float64()
		return vall, err
	case string:
		switch strings.ToLower(val) {
		case "nan":
			return math.NaN(), nil
		case "infinity":
			return math.Inf(1), nil
		case "-infinity":
			return math.Inf(-1), nil
		}
	}
	return 0, xerrors.Errorf("unknown double precision value: %v (%T)", in, in)

}

type NumRange struct {
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (r *NumRange) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (interface{}, error) {
	return typeutil.NumRangeFromDebezium(in)
}

type TimestampWithTimeZone struct {
	debeziumcommon.StringToString
	debeziumcommon.YTTypeTimestamp
	debeziumcommon.FieldReceiverMarker
}

func (t *TimestampWithTimeZone) IsMatched(originalType *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return postgres.IsPgTypeTimestampWithTimeZone(originalType.OriginalType)
}

func (t *TimestampWithTimeZone) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (time.Time, error) {
	timestamp, err := typeutil.ParsePgDateTimeWithTimezone(in)
	if err != nil {
		return time.Time{}, xerrors.Errorf("unable to parse timestamp with timezone: %s, err: %w", in, err)
	}

	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), timestamp.Second(), timestamp.Nanosecond(), time.UTC), nil
}

type Enum struct {
	debeziumcommon.StringToString
	debeziumcommon.YTTypeString
	debeziumcommon.FieldReceiverMarker
}

func (t *Enum) IsMatched(_ *debeziumcommon.OriginalTypeInfo, debeziumSchema *debeziumcommon.Schema) bool {
	return debeziumSchema.Name == "io.debezium.data.Enum"
}

func (t *Enum) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (string, error) {
	return in, nil
}

func (t *Enum) AddInfo(schema *debeziumcommon.Schema, outColSchema *abstract.ColSchema) {
	if schema.Parameters == nil || schema.Parameters.Allowed == "" {
		return
	}
	outColSchema.Properties = map[abstract.PropertyKey]interface{}{postgres.EnumAllValues: strings.Split(schema.Parameters.Allowed, ",")}
}

type TSRange struct {
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (r *TSRange) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (interface{}, error) {
	result, err := typeutil.TSRangeUnquote(in)
	if err != nil {
		return "", xerrors.Errorf("unable to unquote tsrange: %s, err: %w", in, err)
	}
	return result, nil
}

type TSTZRange struct {
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (r *TSTZRange) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (interface{}, error) {
	result, err := typeutil.TstZRangeUnquote(in)
	if err != nil {
		return "", xerrors.Errorf("unable to unquote tstzrange: %s, err: %w", in, err)
	}
	return result, nil
}

type HStore struct {
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (p *HStore) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (interface{}, error) {
	var result interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

type CIText struct {
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (t *CIText) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (interface{}, error) {
	return in, nil
}

// bytes

type DebeziumBuf struct {
	debeziumcommon.StringToString
	debeziumcommon.YTTypeBytes
	debeziumcommon.FieldReceiverMarker
}

func (b *DebeziumBuf) IsMatched(_ *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema) bool {
	return schema.Name == "io.debezium.data.Bits"
}

func (b *DebeziumBuf) Do(in string, _ *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema, _ bool) (string, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return "", xerrors.Errorf("unable to decode base64: %s, err: %w", in, err)
	}
	resultBuf = typeutil.ReverseBytesArr(resultBuf)
	if schema.Parameters != nil && schema.Parameters.Length != "" {
		length, err := strconv.Atoi(schema.Parameters.Length)
		if err != nil {
			return "", xerrors.Errorf("unable to parse length: %s, err: %w", schema.Parameters.Length, err)
		}
		result := typeutil.BufToChangeItemsBits(resultBuf)
		return result[len(result)-length:], nil
	} else {
		return "", xerrors.Errorf("unable to find length of io.debezium.data.Bits: %s, err: %w", schema.Parameters, err)
	}
}

//---------------------------------------------------------------------------------------------------------------------
// things to fix YTType

type BitVarying struct {
	debeziumcommon.StringToString
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (d *BitVarying) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (string, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return "", xerrors.Errorf("unable to decode base64: %s, err: %w", in, err)
	}
	return typeutil.BufToChangeItemsBits(resultBuf), nil
}

type BitN struct {
	debeziumcommon.StringToString
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (d *BitN) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (string, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return "", xerrors.Errorf("unable to decode base64: %s, err: %w", in, err)
	}
	return typeutil.BufToChangeItemsBits(resultBuf), nil
}

type Oid struct {
	debeziumcommon.Int64ToInt64
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (d *Oid) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (int64, error) {
	return in, nil
}

type StringButYTAny struct {
	debeziumcommon.StringToStringDefault
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

type Point struct {
	p debeziumcommon.Point
	debeziumcommon.AnyToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (d *Point) Do(in interface{}, originalTypeInfo *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema, intoArr bool) (interface{}, error) {
	result, err := d.p.Do(in, originalTypeInfo, schema, intoArr)
	return result, err
}

func (d *Point) AddInfo(_ *debeziumcommon.Schema, colSchema *abstract.ColSchema) {
	colSchema.DataType = string(ytschema.TypeString)
}

type Decimal struct {
	p debeziumcommon.Decimal
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeFloat64
	debeziumcommon.FieldReceiverMarker
}

func (d *Decimal) IsMatched(_ *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema) bool {
	return schema.Name == "org.apache.kafka.connect.data.Decimal"
}

func (d *Decimal) Do(in string, originalType *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema, intoArr bool) (interface{}, error) {
	result, err := d.p.Do(in, originalType, schema, intoArr)
	return json.Number(result), err
}
