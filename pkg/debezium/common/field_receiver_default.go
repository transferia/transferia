package common

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium/typeutil"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var TypeToDefault = map[KafkaType]FieldReceiver{
	KafkaTypeInt8:    new(Int8ToInt8Default),
	KafkaTypeInt16:   new(Int16ToInt16Default),
	KafkaTypeInt32:   new(IntToInt32Default),
	KafkaTypeInt64:   new(Int64ToInt64Default),
	KafkaTypeBoolean: new(BooleanToBooleanDefault),
	KafkaTypeString:  new(StringToStringDefault),
	KafkaTypeFloat32: new(Float64ToFloat64Default),
	KafkaTypeFloat64: new(Float64ToFloat64Default),
	KafkaTypeStruct: &FieldReceiverMatchers{
		Matchers: []FieldReceiverMatcher{new(Point), new(VariableScaleDecimal)},
	},
	KafkaTypeBytes: &FieldReceiverMatchers{
		Matchers: []FieldReceiverMatcher{new(Decimal), new(StringToBytesDefault)},
	},
}

// TODO - maybe move here:
//     - DebeziumBuf

//---------------------------------------------------------------------------------------------------------------------
// Interfaces for FieldReceiver for any allowed types KafkaType->GolangTypeWhichInColumnValues
//
// This is about intput/output values
//     - input: int64/bool/float64/string, Struct/Any - is interface{}
//
// KafkaType should correspond first argument type
// GolangTypeWhichInColumnValues should correspond first output type
//
// for example: Int16ToInt8Default - it's kafka type Int16 to yt type Int8

type Int8ToInt8Default struct {
	Int8ToInt8
	YTTypeInt8
	FieldReceiverMarker
}

func (d *Int8ToInt8Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (int8, error) {
	return int8(in), nil
}

//---

type Int8ToUint8Default struct {
	Int8ToUint8
	YTTypeUint8
	FieldReceiverMarker
}

func (d *Int8ToUint8Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (uint8, error) {
	return uint8(in), nil
}

//---

type Int16ToUint8Default struct {
	Int16ToUint8
	YTTypeUint8
	FieldReceiverMarker
}

func (d *Int16ToUint8Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (uint8, error) {
	return uint8(in), nil
}

//---

type Int16ToInt16Default struct {
	Int16ToInt16
	YTTypeInt16
	FieldReceiverMarker
}

func (d *Int16ToInt16Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (int16, error) {
	return int16(in), nil
}

//---

type Int16ToInt8Default struct {
	Int16ToInt8
	YTTypeInt8
	FieldReceiverMarker
}

func (d *Int16ToInt8Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (int8, error) {
	return int8(in), nil
}

//---

type Int16ToUint16Default struct {
	Int16ToUint16
	YTTypeUint16
	FieldReceiverMarker
}

func (d *Int16ToUint16Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (uint16, error) {
	return uint16(in), nil
}

//---

type IntToInt32Default struct {
	IntToInt32
	YTTypeInt32
	FieldReceiverMarker
}

func (d *IntToInt32Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (int32, error) {
	return int32(in), nil
}

//---

type IntToUint32Default struct {
	IntToUint32
	YTTypeUint32
	FieldReceiverMarker
}

func (d *IntToUint32Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (uint32, error) {
	return uint32(in), nil
}

//---

type IntToUint16Default struct {
	IntToUint16
	YTTypeUint16
	FieldReceiverMarker
}

func (d *IntToUint16Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (uint16, error) {
	return uint16(in), nil
}

//---

type IntToStringDefault struct {
	IntToString
	YTTypeString
	FieldReceiverMarker
}

func (d *IntToStringDefault) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (string, error) {
	return fmt.Sprintf("%d", in), nil
}

//---

type Int64ToInt64Default struct {
	Int64ToInt64
	YTTypeInt64
	FieldReceiverMarker
}

func (d *Int64ToInt64Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (int64, error) {
	return in, nil
}

//---

type Int64ToUint64Default struct {
	Int64ToUint64
	YTTypeUint64
	FieldReceiverMarker
}

func (d *Int64ToUint64Default) Do(in int64, _ *OriginalTypeInfo, _ *Schema, _ bool) (uint64, error) {
	return uint64(in), nil
}

//---

type BooleanToBooleanDefault struct {
	BooleanToBoolean
	YTTypeBoolean
	FieldReceiverMarker
}

func (d *BooleanToBooleanDefault) Do(in bool, _ *OriginalTypeInfo, _ *Schema, _ bool) (bool, error) {
	return in, nil
}

//---

type Float64ToFloat32Default struct {
	Float64ToFloat32
	YTTypeFloat32
	FieldReceiverMarker
}

func (d *Float64ToFloat32Default) Do(in float64, _ *OriginalTypeInfo, _ *Schema, _ bool) (float32, error) {
	return float32(in), nil
}

//---

type Float64ToFloat64Default struct {
	Float64ToFloat64
	YTTypeFloat64
	FieldReceiverMarker
}

func (d *Float64ToFloat64Default) Do(in float64, _ *OriginalTypeInfo, _ *Schema, _ bool) (float64, error) {
	return in, nil
}

//---

type StringToAnyDefault struct {
	StringToAny
	YTTypeAny
	FieldReceiverMarker
}

func (d *StringToAnyDefault) Do(in string, _ *OriginalTypeInfo, _ *Schema, _ bool) (interface{}, error) {
	return in, nil
}

//---

type StringToStringDefault struct {
	StringToString
	YTTypeString
	FieldReceiverMarker
}

func (d *StringToStringDefault) Do(in string, _ *OriginalTypeInfo, _ *Schema, _ bool) (string, error) {
	return in, nil
}

//---

type StringToBytesDefault struct {
	StringToBytes
	YTTypeBytes
	FieldReceiverMarker
}

func (d *StringToBytesDefault) IsMatched(_ *OriginalTypeInfo, _ *Schema) bool {
	return true
}

func (d *StringToBytesDefault) Do(in string, _ *OriginalTypeInfo, _ *Schema, _ bool) ([]byte, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return nil, xerrors.Errorf("unable to decode base64: %s, err: %w", in, err)
	}
	return resultBuf, nil
}

//---

type Point struct {
	AnyToAny
	YTTypeString
	FieldReceiverMarker
}

func (p *Point) IsMatched(_ *OriginalTypeInfo, schema *Schema) bool {
	return schema.Name == "io.debezium.data.geometry.Point"
}

func (p *Point) Do(in interface{}, _ *OriginalTypeInfo, _ *Schema, _ bool) (interface{}, error) {
	vv := in.(map[string]interface{})
	if _, ok := vv["x"]; !ok {
		return "", xerrors.New("unable to find 'x' in point")
	}
	if _, ok := vv["y"]; !ok {
		return "", xerrors.New("unable to find 'y' in point")
	}
	return fmt.Sprintf("(%v,%v)", vv["x"], vv["y"]), nil
}

func (p *Point) AddInfo(_ *Schema, colSchema *abstract.ColSchema) {
	colSchema.DataType = string(ytschema.TypeString)
}

type VariableScaleDecimal struct {
	AnyToDouble
	YTTypeFloat64
	FieldReceiverMarker
}

func (d *VariableScaleDecimal) IsMatched(_ *OriginalTypeInfo, schema *Schema) bool {
	return schema.Name == "io.debezium.data.VariableScaleDecimal"
}

func (d *VariableScaleDecimal) Do(in interface{}, _ *OriginalTypeInfo, _ *Schema, _ bool) (json.Number, error) {
	currStruct := in.(map[string]interface{})
	var based64Buf string
	var scale int
	if based64Buf_, ok := currStruct["value"]; ok {
		based64Buf = based64Buf_.(string)
	} else {
		return "", xerrors.New("unable to find 'value' for numeric type")
	}
	if scaleInterface, ok := currStruct["scale"]; ok {
		scale_, err := scaleInterface.(json.Number).Int64()
		if err != nil {
			return "", xerrors.New("unable to parse integer value of 'scale'")
		}
		scale = int(scale_)
	}
	result, err := typeutil.Base64ToNumeric(based64Buf, scale)
	if err != nil {
		return "", xerrors.New("unable to convert base64 to numeric")
	}
	return json.Number(result), nil
}

type Decimal struct {
	StringToString
	YTTypeString
	FieldReceiverMarker
}

func (d *Decimal) IsMatched(_ *OriginalTypeInfo, schema *Schema) bool {
	return schema.Name == "org.apache.kafka.connect.data.Decimal"
}

func (d *Decimal) Do(in string, originalType *OriginalTypeInfo, schema *Schema, _ bool) (string, error) {
	var scale int
	if schema.Parameters != nil && schema.Parameters.Scale != "" {
		var err error
		scale, err = strconv.Atoi(schema.Parameters.Scale)
		if err != nil {
			return "", xerrors.Errorf("unable to parse scale: %s, err: %w", schema.Parameters.Scale, err)
		}
	}
	resultStr, err := typeutil.Base64ToNumeric(in, scale)
	if err != nil {
		return "", xerrors.Errorf("unable to convert base64 to numeric: %s, err: %w", in, err)
	}
	if originalType.OriginalType == "pg:money" {
		resultStr = "$" + resultStr
	}
	return resultStr, nil
}
