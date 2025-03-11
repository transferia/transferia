package common

import (
	"encoding/json"
	"time"

	"github.com/transferria/transferria/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

//---
// main interface for FieldReceiver objects

type FieldReceiver interface {
	IsFieldReceiver()
	YTTypeStorer
}

//---
// struct to embed into field_receiver objects

type FieldReceiverMarker struct{}

func (r *FieldReceiverMarker) IsFieldReceiver() {}

//---
// useful declarations

const DTMatchByFunc = "DATA_TRANSFER_CHECK_BY_FUNC"

type NotDefaultReceiverDescription map[KafkaType]map[string]FieldReceiver

//---------------------------------------------------------------------------------------------------------------------
// all possible converting: KafkaType->YtType

type ResultTypes struct {
	ResultType            string
	AlternativeResultType []string
}

var KafkaTypeToResultYTTypes = map[KafkaType]ResultTypes{
	KafkaTypeInt8: {string(ytschema.TypeInt8), []string{
		string(ytschema.TypeUint8),
	}},
	KafkaTypeInt16: {string(ytschema.TypeInt16), []string{
		string(ytschema.TypeInt8),
		string(ytschema.TypeUint8),
		string(ytschema.TypeUint16),
	}},
	KafkaTypeInt32: {string(ytschema.TypeInt32), []string{
		string(ytschema.TypeString),
		string(ytschema.TypeUint16),
		string(ytschema.TypeUint32),
		string(ytschema.TypeDate),
	}},
	KafkaTypeInt64: {string(ytschema.TypeInt64), []string{
		string(ytschema.TypeString),
		string(ytschema.TypeUint32),
		string(ytschema.TypeInt32),
		string(ytschema.TypeUint64),
		string(ytschema.TypeDatetime),
		string(ytschema.TypeTimestamp),
		string(ytschema.TypeInterval),
		string(ytschema.TypeAny),
	}},
	KafkaTypeBoolean: {string(ytschema.TypeBoolean), []string{
		string(ytschema.TypeBytes),
		string(ytschema.TypeAny),
	}},
	KafkaTypeBytes: {string(ytschema.TypeBytes), []string{
		string(ytschema.TypeString),
		string(ytschema.TypeFloat64),
		string(ytschema.TypeAny),
	}},
	KafkaTypeString: {string(ytschema.TypeString), []string{
		string(ytschema.TypeTimestamp),
		string(ytschema.TypeAny),
	}},
	KafkaTypeFloat32: {string(ytschema.TypeFloat64), []string{
		string(ytschema.TypeFloat32),
	}},
	KafkaTypeFloat64: {string(ytschema.TypeFloat64), []string{}},
	KafkaTypeStruct: {string(ytschema.TypeFloat64), []string{
		string(ytschema.TypeString),
		string(ytschema.TypeAny),
	}},
	KafkaTypeArray: {string(ytschema.TypeAny), []string{}},
}

//---------------------------------------------------------------------------------------------------------------------
//

type FieldReceiverMatcher interface {
	FieldReceiver
	IsMatched(originalType *OriginalTypeInfo, schema *Schema) bool
}

type FieldReceiverMatchers struct {
	Matchers []FieldReceiverMatcher
}

func (m *FieldReceiverMatchers) IsFieldReceiver() {}

func (m *FieldReceiverMatchers) YTType() string {
	panic("should never be called")
}

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

type Int8ToInt8 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (int8, error)
}

type Int8ToUint8 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (uint8, error)
}

type Int16ToUint8 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (uint8, error)
}

type Int16ToInt16 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (int16, error)
}

type Int16ToUint16 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (uint16, error)
}

type Int16ToInt8 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (int8, error)
}

// IntToInt32
// special case - generalization Int*ToInt32
type IntToInt32 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (int32, error)
}

type IntToUint32 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (uint32, error)
}

type IntToUint16 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (uint16, error)
}

// IntToString
// special case - generalization Int*ToString
type IntToString interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (string, error)
}

type Int64ToInt64 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (int64, error)
}

type DurationToInt64 interface {
	Do(time.Duration, *OriginalTypeInfo, *Schema, bool) (int64, error)
}

type Int64ToUint64 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (uint64, error)
}

type Int64ToUint32 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (uint32, error)
}

type Int64ToInt32 interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (int32, error)
}

type Int64ToTime interface {
	Do(int64, *OriginalTypeInfo, *Schema, bool) (time.Time, error)
}

type BooleanToBoolean interface {
	Do(bool, *OriginalTypeInfo, *Schema, bool) (bool, error)
}

type BooleanToInt8 interface {
	Do(bool, *OriginalTypeInfo, *Schema, bool) (int8, error)
}

type BooleanToBytes interface {
	Do(bool, *OriginalTypeInfo, *Schema, bool) ([]byte, error)
}

type BooleanToString interface {
	Do(bool, *OriginalTypeInfo, *Schema, bool) (string, error)
}

type StringToString interface {
	Do(string, *OriginalTypeInfo, *Schema, bool) (string, error)
}

type StringToTime interface {
	Do(string, *OriginalTypeInfo, *Schema, bool) (time.Time, error)
}

type StringToBytes interface {
	Do(string, *OriginalTypeInfo, *Schema, bool) ([]byte, error)
}

type StringToAny interface {
	Do(string, *OriginalTypeInfo, *Schema, bool) (interface{}, error)
}

type Float64ToFloat32 interface {
	Do(float64, *OriginalTypeInfo, *Schema, bool) (float32, error)
}

type Float64ToFloat64 interface {
	Do(float64, *OriginalTypeInfo, *Schema, bool) (float64, error)
}

type StructToFloat64 interface {
	Do(interface{}, *OriginalTypeInfo, *Schema, bool) (float64, error)
}

type StructToString interface {
	Do(interface{}, *OriginalTypeInfo, *Schema, bool) (string, error)
}

// two workarounds

// AnyToDouble
// special for io.debezium.data.VariableScaleDecimal
type AnyToDouble interface {
	Do(interface{}, *OriginalTypeInfo, *Schema, bool) (json.Number, error)
}

// AnyToAny
// it's: ArrayToAny & StructToAny
type AnyToAny interface {
	Do(interface{}, *OriginalTypeInfo, *Schema, bool) (interface{}, error) // string ret_val: result_type
}

// ContainsColSchemaAdditionalInfo
// modifies ColSchema, adds special parameters
type ContainsColSchemaAdditionalInfo interface {
	AddInfo(*Schema, *abstract.ColSchema)
}
