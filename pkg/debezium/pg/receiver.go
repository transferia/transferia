package pg

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debezium_common "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/typeutil"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/util/xlocale"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

//---------------------------------------------------------------------------------------------------------------------
// pg non-default converting

var KafkaTypeToOriginalTypeToFieldReceiverFunc = map[debezium_common.KafkaType]map[string]debezium_common.FieldReceiver{
	debezium_common.KafkaTypeInt32: {
		"pg:date": new(Date),
		debezium_common.DTMatchByFunc: &debezium_common.FieldReceiverMatchers{
			Matchers: []debezium_common.FieldReceiverMatcher{new(TimeWithoutTimeZone)},
		},
	},
	debezium_common.KafkaTypeInt64: {
		"pg:interval": new(Interval),
		"pg:oid":      new(Oid),
		debezium_common.DTMatchByFunc: &debezium_common.FieldReceiverMatchers{
			Matchers: []debezium_common.FieldReceiverMatcher{new(TimestampWithoutTimeZone), new(TimeWithoutTimeZone2)},
		},
	},
	debezium_common.KafkaTypeBoolean: {
		"pg:bit(1)": new(Bit1),
	},
	debezium_common.KafkaTypeBytes: {
		"pg:bit":         new(BitN),
		"pg:bit varying": new(BitVarying),
		"pg:money":       new(debezium_common.Decimal),
		debezium_common.DTMatchByFunc: &debezium_common.FieldReceiverMatchers{
			Matchers: []debezium_common.FieldReceiverMatcher{new(Decimal), new(DebeziumBuf)},
		},
	},
	debezium_common.KafkaTypeFloat64: {
		"pg:double precision": new(DoublePrecision),
	},
	debezium_common.KafkaTypeString: {
		"pg:inet":                new(Inet),
		"pg:int4range":           new(debezium_common.StringToAnyDefault),
		"pg:int8range":           new(debezium_common.StringToAnyDefault),
		"pg:json":                new(JSON),
		"pg:jsonb":               new(JSON),
		"pg:numrange":            new(NumRange),
		"pg:tsrange":             new(TSRange),
		"pg:tstzrange":           new(TSTZRange),
		"pg:xml":                 new(debezium_common.StringToAnyDefault),
		"pg:citext":              new(CIText), // TODO - to remove after summer 2026, bcs we moved to 'pg:USER-DEFINED:citext' here
		"pg:USER-DEFINED:citext": new(CIText),
		"pg:hstore":              new(HStore), // TODO - to remove after summer 2026, bcs we moved to 'pg:USER-DEFINED:hstore' here
		"pg:USER-DEFINED:hstore": new(HStore),
		"pg:macaddr":             new(StringButYTAny),
		"pg:cidr":                new(StringButYTAny),
		"pg:character":           new(StringButYTAny),
		"pg:daterange":           new(StringButYTAny),
		debezium_common.DTMatchByFunc: &debezium_common.FieldReceiverMatchers{
			Matchers: []debezium_common.FieldReceiverMatcher{new(TimestampWithTimeZone), new(Enum)},
		},
	},
	debezium_common.KafkaTypeStruct: {
		"pg:point": new(Point),
	},
}

//---------------------------------------------------------------------------------------------------------------------
// int32

type Date struct {
	debezium_common.Int64ToTime
	debezium_common.YTTypeDate
	debezium_common.FieldReceiverMarker
}

func (d *Date) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (time.Time, error) {
	return time.Unix(in*3600*24, 0).UTC(), nil
}

type TimeWithoutTimeZone struct {
	debezium_common.IntToString
	debezium_common.YTTypeString
	debezium_common.FieldReceiverMarker
}

func (t *TimeWithoutTimeZone) IsMatched(originalType *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return provider_postgres.IsPgTypeTimeWithoutTimeZone(originalType.OriginalType)
}

func (t *TimeWithoutTimeZone) Do(in int64, originalType *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (string, error) {
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
	debezium_common.IntToString
	debezium_common.YTTypeString
	debezium_common.FieldReceiverMarker
}

func (i *Interval) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (string, error) {
	return typeutil.EmitPostgresInterval(in), nil
}

type TimestampWithoutTimeZone struct {
	debezium_common.Int64ToTime
	debezium_common.YTTypeTimestamp
	debezium_common.FieldReceiverMarker
}

func (t *TimestampWithoutTimeZone) IsMatched(originalType *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return provider_postgres.IsPgTypeTimestampWithoutTimeZone(originalType.OriginalType)
}

var OriginalTypePropertyTimeZone = "timezone"

func (t *TimestampWithoutTimeZone) Do(in int64, originalType *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, intoArr bool) (time.Time, error) {
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
	debezium_common.IntToString
	debezium_common.YTTypeString
	debezium_common.FieldReceiverMarker
}

func (t *TimeWithoutTimeZone2) IsMatched(originalType *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return provider_postgres.IsPgTypeTimeWithoutTimeZone(originalType.OriginalType)
}

func (t *TimeWithoutTimeZone2) Do(in int64, originalType *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, intoArr bool) (string, error) {
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
	debezium_common.IntToString
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (b *Bit1) Do(in bool, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (string, error) {
	if in {
		return "1", nil
	} else {
		return "0", nil
	}
}

// string

type Inet struct {
	debezium_common.StringToString
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (i *Inet) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (string, error) {
	if !strings.Contains(in, "/") {
		in += "/32"
	}
	return in, nil
}

type JSON struct {
	debezium_common.StringToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (i *JSON) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (interface{}, error) {
	var result interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&result); err != nil {
		return "", err
	}
	return result, nil
}

type DoublePrecision struct {
	debezium_common.AnyToDouble
	debezium_common.YTTypeFloat64
	debezium_common.FieldReceiverMarker
}

func (p *DoublePrecision) Do(in interface{}, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (float64, error) {
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
	debezium_common.StringToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (r *NumRange) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (interface{}, error) {
	return typeutil.NumRangeFromDebezium(in)
}

type TimestampWithTimeZone struct {
	debezium_common.StringToString
	debezium_common.YTTypeTimestamp
	debezium_common.FieldReceiverMarker
}

func (t *TimestampWithTimeZone) IsMatched(originalType *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return provider_postgres.IsPgTypeTimestampWithTimeZone(originalType.OriginalType)
}

func (t *TimestampWithTimeZone) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (time.Time, error) {
	timestamp, err := typeutil.ParsePgDateTimeWithTimezone(in)
	if err != nil {
		return time.Time{}, xerrors.Errorf("unable to parse timestamp with timezone: %s, err: %w", in, err)
	}

	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), timestamp.Second(), timestamp.Nanosecond(), time.UTC), nil
}

type Enum struct {
	debezium_common.StringToString
	debezium_common.YTTypeString
	debezium_common.FieldReceiverMarker
}

func (t *Enum) IsMatched(_ *debezium_common.OriginalTypeInfo, debeziumSchema *debezium_common.Schema) bool {
	return debeziumSchema.Name == "io.debezium.data.Enum"
}

func (t *Enum) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (string, error) {
	return in, nil
}

func (t *Enum) AddInfo(schema *debezium_common.Schema, outColSchema *abstract.ColSchema) {
	if schema.Parameters == nil || schema.Parameters.Allowed == "" {
		return
	}
	outColSchema.Properties = map[abstract.PropertyKey]interface{}{provider_postgres.EnumAllValues: strings.Split(schema.Parameters.Allowed, ",")}
}

type TSRange struct {
	debezium_common.StringToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (r *TSRange) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (interface{}, error) {
	result, err := typeutil.TSRangeUnquote(in)
	if err != nil {
		return "", xerrors.Errorf("unable to unquote tsrange: %s, err: %w", in, err)
	}
	return result, nil
}

type TSTZRange struct {
	debezium_common.StringToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (r *TSTZRange) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (interface{}, error) {
	result, err := typeutil.TstZRangeUnquote(in)
	if err != nil {
		return "", xerrors.Errorf("unable to unquote tstzrange: %s, err: %w", in, err)
	}
	return result, nil
}

type HStore struct {
	debezium_common.StringToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (p *HStore) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (interface{}, error) {
	var result interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

type CIText struct {
	debezium_common.StringToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (t *CIText) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (interface{}, error) {
	return in, nil
}

// bytes

type DebeziumBuf struct {
	debezium_common.StringToString
	debezium_common.YTTypeBytes
	debezium_common.FieldReceiverMarker
}

func (b *DebeziumBuf) IsMatched(_ *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema) bool {
	return schema.Name == "io.debezium.data.Bits"
}

func (b *DebeziumBuf) Do(in string, _ *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema, _ bool) (string, error) {
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
	debezium_common.StringToString
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (d *BitVarying) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (string, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return "", xerrors.Errorf("unable to decode base64: %s, err: %w", in, err)
	}
	return typeutil.BufToChangeItemsBits(resultBuf), nil
}

type BitN struct {
	debezium_common.StringToString
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (d *BitN) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (string, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return "", xerrors.Errorf("unable to decode base64: %s, err: %w", in, err)
	}
	return typeutil.BufToChangeItemsBits(resultBuf), nil
}

type Oid struct {
	debezium_common.Int64ToInt64
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (d *Oid) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (int64, error) {
	return in, nil
}

type StringButYTAny struct {
	debezium_common.StringToStringDefault
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

type Point struct {
	p debezium_common.Point
	debezium_common.AnyToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (d *Point) Do(in interface{}, originalTypeInfo *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema, intoArr bool) (interface{}, error) {
	result, err := d.p.Do(in, originalTypeInfo, schema, intoArr)
	return result, err
}

func (d *Point) AddInfo(_ *debezium_common.Schema, colSchema *abstract.ColSchema) {
	colSchema.DataType = string(ytschema.TypeString)
}

type Decimal struct {
	p debezium_common.Decimal
	debezium_common.StringToAny
	debezium_common.YTTypeFloat64
	debezium_common.FieldReceiverMarker
}

func (d *Decimal) IsMatched(_ *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema) bool {
	return schema.Name == "org.apache.kafka.connect.data.Decimal"
}

func (d *Decimal) Do(in string, originalType *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema, intoArr bool) (interface{}, error) {
	result, err := d.p.Do(in, originalType, schema, intoArr)
	return json.Number(result), err
}
