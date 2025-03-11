package mysql

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	"github.com/transferria/transferria/pkg/debezium/typeutil"
	"github.com/transferria/transferria/pkg/util/jsonx"
)

//---------------------------------------------------------------------------------------------------------------------
// mysql non-default converting

var KafkaTypeToOriginalTypeToFieldReceiverFunc = map[debeziumcommon.KafkaType]map[string]debeziumcommon.FieldReceiver{
	debeziumcommon.KafkaTypeBoolean: {
		"mysql:tinyint(1)": new(TinyInt1),
		"mysql:bit(1)":     new(Bit1),
	},
	debeziumcommon.KafkaTypeInt16: {
		"mysql:smallint": new(debeziumcommon.Int16ToInt16Default),
		debeziumcommon.DTMatchByFunc: &debeziumcommon.FieldReceiverMatchers{
			Matchers: []debeziumcommon.FieldReceiverMatcher{new(TinyIntSigned), new(TinyIntUnsigned)},
		},
	},
	debeziumcommon.KafkaTypeInt32: {
		"mysql:date": new(Date),
		"mysql:year": new(debeziumcommon.IntToStringDefault),
		debeziumcommon.DTMatchByFunc: &debeziumcommon.FieldReceiverMatchers{
			Matchers: []debeziumcommon.FieldReceiverMatcher{new(SmallIntSigned), new(SmallIntUnsigned)},
		},
	},
	debeziumcommon.KafkaTypeInt64: {
		"mysql:time":     new(Time),
		"mysql:datetime": new(Datetime),
		debeziumcommon.DTMatchByFunc: &debeziumcommon.FieldReceiverMatchers{
			Matchers: []debeziumcommon.FieldReceiverMatcher{new(IntSigned), new(IntUnsigned), new(Int64Unsigned)},
		},
	},
	debeziumcommon.KafkaTypeString: {
		"mysql:timestamp": new(Timestamp),
		"mysql:json":      new(JSON),
	},
	debeziumcommon.KafkaTypeBytes: {
		"mysql:decimal": new(Decimal),
		debeziumcommon.DTMatchByFunc: &debeziumcommon.FieldReceiverMatchers{
			Matchers: []debeziumcommon.FieldReceiverMatcher{new(DebeziumBuf)},
		},
	},
}

//---------------------------------------------------------------------------------------------------------------------
// bool

type TinyInt1 struct {
	debeziumcommon.BooleanToInt8
	debeziumcommon.YTTypeBoolean
	debeziumcommon.FieldReceiverMarker
}

func (d *TinyInt1) Do(in bool, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (int8, error) {
	if in {
		return 1, nil
	} else {
		return 0, nil
	}
}

type Bit1 struct {
	debeziumcommon.BooleanToBytes
	debeziumcommon.YTTypeBytes
	debeziumcommon.FieldReceiverMarker
}

func (d *Bit1) Do(in bool, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) ([]byte, error) {
	if in {
		return []byte{0, 0, 0, 0, 0, 0, 0, 1}, nil
	} else {
		return []byte{0, 0, 0, 0, 0, 0, 0, 0}, nil
	}
}

// int16

type TinyIntUnsigned struct {
	debeziumcommon.Int16ToUint8
	debeziumcommon.YTTypeUint8
	debeziumcommon.FieldReceiverMarker
}

func (d *TinyIntUnsigned) IsMatched(originalTypeInfo *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:tinyint") && strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned")
}

func (d *TinyIntUnsigned) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (uint8, error) {
	return uint8(in), nil
}

type TinyIntSigned struct {
	debeziumcommon.Int16ToInt8
	debeziumcommon.YTTypeInt8
	debeziumcommon.FieldReceiverMarker
}

func (d *TinyIntSigned) IsMatched(originalTypeInfo *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:tinyint") && (!strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned"))
}

func (d *TinyIntSigned) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (int8, error) {
	return int8(in), nil
}

// int32

type SmallIntUnsigned struct {
	debeziumcommon.Int16ToUint16
	debeziumcommon.YTTypeUint16
	debeziumcommon.FieldReceiverMarker
}

func (d *SmallIntUnsigned) IsMatched(originalTypeInfo *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:smallint") && strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned")
}

func (d *SmallIntUnsigned) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (uint16, error) {
	return uint16(in), nil
}

type SmallIntSigned struct {
	debeziumcommon.Int16ToInt16
	debeziumcommon.YTTypeInt16
	debeziumcommon.FieldReceiverMarker
}

func (d *SmallIntSigned) IsMatched(originalTypeInfo *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:smallint") && (!strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned"))
}

func (d *SmallIntSigned) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (int16, error) {
	return int16(in), nil
}

type Date struct {
	debeziumcommon.Int64ToTime
	debeziumcommon.YTTypeString
	debeziumcommon.FieldReceiverMarker
}

func (d *Date) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (time.Time, error) {
	return time.Unix(in*86400, 0).UTC(), nil
}

type IntUnsigned struct {
	debeziumcommon.Int64ToUint32
	debeziumcommon.YTTypeUint32
	debeziumcommon.FieldReceiverMarker
}

func (d *IntUnsigned) IsMatched(originalTypeInfo *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:int") && strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned")
}

func (d *IntUnsigned) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (uint32, error) {
	return uint32(in), nil
}

type IntSigned struct {
	debeziumcommon.Int64ToInt32
	debeziumcommon.YTTypeInt32
	debeziumcommon.FieldReceiverMarker
}

func (d *IntSigned) IsMatched(originalTypeInfo *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:int") && (!strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned"))
}

func (d *IntSigned) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (int32, error) {
	return int32(in), nil
}

// int64

type Int64Unsigned struct {
	debeziumcommon.Int64ToUint64
	debeziumcommon.YTTypeUint64
	debeziumcommon.FieldReceiverMarker
}

func (d *Int64Unsigned) IsMatched(originalTypeInfo *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:bigint") && strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned")
}

func (d *Int64Unsigned) Do(in int64, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (uint64, error) {
	return uint64(in), nil
}

type Time struct {
	debeziumcommon.IntToString
	debeziumcommon.YTTypeString
	debeziumcommon.FieldReceiverMarker
}

func (d *Time) Do(in int64, originalTypeInfo *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (string, error) {
	precision, err := typeutil.ExtractParameter(originalTypeInfo.OriginalType)
	if err != nil {
		precision = 0
	}
	hh := in / (3600 * 1000000)
	mm := (in % (3600 * 1000000)) / (60 * 1000000)
	ss := (in % (60 * 1000000)) / 1000000
	fraction := in % 1000000

	result := fmt.Sprintf(`%02d:%02d:%02d`, hh, mm, ss)
	return result + typeutil.MakeFractionSecondSuffix(fraction, precision), nil
}

type Datetime struct {
	debeziumcommon.Int64ToTime
	debeziumcommon.YTTypeString
	debeziumcommon.FieldReceiverMarker
}

func (d *Datetime) Do(in int64, originalType *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (time.Time, error) {
	datetimePrecisionInt := typeutil.GetTimePrecision(originalType.OriginalType)
	if datetimePrecisionInt == -1 {
		datetimePrecisionInt = 0
	}
	var result time.Time
	if datetimePrecisionInt >= 4 && datetimePrecisionInt <= 6 {
		result = time.Unix(in/1000000, (in%1000000)*1000).UTC()
	} else {
		result = time.Unix(in/1000, (in%1000)*1000000).UTC()
	}
	return result, nil
}

// string

type Timestamp struct {
	debeziumcommon.StringToTime
	debeziumcommon.YTTypeTimestamp
	debeziumcommon.FieldReceiverMarker
}

func (d *Timestamp) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (time.Time, error) {
	return typeutil.ParseTimestamp(in)
}

type JSON struct {
	debeziumcommon.StringToAny
	debeziumcommon.YTTypeAny
	debeziumcommon.FieldReceiverMarker
}

func (j *JSON) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) (interface{}, error) {
	var result interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&result); err != nil {
		return "", err
	}
	return result, nil
}

// bytes

type DebeziumBuf struct {
	debeziumcommon.StringToBytes
	debeziumcommon.YTTypeBytes
	debeziumcommon.FieldReceiverMarker
}

func (b *DebeziumBuf) IsMatched(_ *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema) bool {
	return schema.Name == "io.debezium.data.Bits"
}

func (b *DebeziumBuf) Do(in string, _ *debeziumcommon.OriginalTypeInfo, _ *debeziumcommon.Schema, _ bool) ([]byte, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return nil, xerrors.Errorf("unable to decode base64: %s, err: %w", in, err)
	}
	return typeutil.ReverseBytesArr(resultBuf), nil
}

type Decimal struct {
	debeziumcommon.AnyToDouble
	debeziumcommon.YTTypeFloat64
	debeziumcommon.FieldReceiverMarker
}

func (b *Decimal) Do(in interface{}, originalTypeInfo *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema, _ bool) (json.Number, error) {
	decimal := new(debeziumcommon.Decimal)
	resultStr, err := decimal.Do(in.(string), originalTypeInfo, schema, false)
	if err != nil {
		return "", xerrors.Errorf("unable to receive decimal, err: %w", err)
	}
	return json.Number(resultStr), nil
}
